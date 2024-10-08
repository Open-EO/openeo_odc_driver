# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   06/05/2024

import os
import signal
import sys
from flask import Flask, request, jsonify
import json
import requests
import yaml
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import uuid
import copy

from openeo_pg_parser_networkx.graph import OpenEOProcessGraph

import log_jobid
from processing import InitProcesses, JobId, output_format
from sar2cube.utils import sar2cube_collection_extent

from config import *
import pystac

app = Flask(FLASK_APP_NAME)

@app.errorhandler(500)
def error500(error):
    return error, 500

@app.errorhandler(400)
def error400(error):
    return error, 400


@app.route('/graph', methods=['POST'])
def process_graph():
    _log = log_jobid.LogJobID(file=LOG_PATH)
    if not os.path.exists(JOB_LOG_FILE):
        lst = ['job_id', 'pid', 'creation_time']
        df = pd.DataFrame(columns=lst)
        df.to_csv(JOB_LOG_FILE)
    else:
        df = pd.read_csv(JOB_LOG_FILE,index_col=0)
    jsonGraph = request.json
    try:
        _log.debug('Gunicorn worker pid for this job: {}'.format(os.getpid()))
        try:
            job_id = jsonGraph['id']
            _log.set_job_id(job_id)
            _log.info(f"Obtaining job id from graph: {job_id}")
        except Exception as e:
            _log.error(e)
            job_id = 'None'
            _log.set_job_id(job_id)

        JobId(job_id)
        local_time = datetime.now(timezone.utc).astimezone()
        time_string = local_time.isoformat()
        df = df[df['job_id']!=job_id]
        df = pd.concat([df, pd.DataFrame([{'job_id':job_id,'creation_time':time_string,'pid':os.getpid()}])], ignore_index=True)
        df.to_csv(JOB_LOG_FILE)
        is_batch_job = False
        if job_id == "None":
            result_folder_path = RESULT_FOLDER_PATH + str(uuid.uuid4())
        else:
            is_batch_job = True
            result_folder_path = RESULT_FOLDER_PATH + job_id # If it is a batch job, there will be a field with it's id
        try:
            os.mkdir(result_folder_path)
        except Exception as e:
            _log.error(e)
            pass
        try:
            with open(result_folder_path + '/process_graph.json', 'w') as outfile:
                json.dump(jsonGraph, outfile)
        except Exception as e:
            _log.error(e)
            pass
        process_registry = InitProcesses(result_folder_path,is_batch_job)

        original_graph = copy.deepcopy(jsonGraph)

        # Replace load_collection nodes with load_stac
        # Look for the STAC Collection URL from the config
        for p in jsonGraph["process_graph"]:
            if jsonGraph["process_graph"][p]["process_id"] == "load_collection":
                collection_name = jsonGraph["process_graph"][p]["arguments"].pop("id")
                jsonGraph["process_graph"][p]["arguments"]["url"] = EXTERNAL_STAC_COLLECTION[collection_name]["href"]
                if EXTERNAL_STAC_COLLECTION[collection_name]["properties"] is not None:
                    jsonGraph["process_graph"][p]["arguments"]["properties"] = EXTERNAL_STAC_COLLECTION[collection_name]["properties"]
                jsonGraph["process_graph"][p]["process_id"] = "load_stac"

        # Optimize the process graph
        # 1. If the first node is load_stac and the following is resample_spatial, set the resolution and projection
        # directly in load_stac, so that if data pyramids are available (COG), they are used automatically
        pg = OpenEOProcessGraph(jsonGraph)
        nodes_to_mod = []
        for n in pg.G.nodes:
            if pg.G.nodes[n]["process_id"] == "resample_spatial":
                print("Check if the parent node is a load_stac")
                print(n)
                print(pg.G.nodes[n]["resolved_kwargs"]["data"].from_node)
                if "loadstac" in (pg.G.nodes[n]["resolved_kwargs"]["data"].from_node) or "loadcollection" in (pg.G.nodes[n]["resolved_kwargs"]["data"].from_node):
                    load_stac_node = pg.G.nodes[n]["resolved_kwargs"]["data"].from_node.split("-")[0]
                    resample_spatial_node = n.split("-")[0]
                    print("modify load_stac and remove resample_spatial")
                    print(pg.G.nodes[n]["resolved_kwargs"])
                    resolution = pg.G.nodes[n]["resolved_kwargs"]["resolution"]
                    projection = pg.G.nodes[n]["resolved_kwargs"]["projection"]
                    resampling = None
                    if "method" in pg.G.nodes[n]["resolved_kwargs"]:
                        resampling = pg.G.nodes[n]["resolved_kwargels"]["method"]
                    elif resampling == "near" or resampling is None:
                        resampling = "nearest"
                    print("Check if it is a result node")
                    result_node = False
                    if pg.G.nodes[n]["result"]:
                        result_node = True
                    nodes_to_mod.append([result_node,load_stac_node,resample_spatial_node,resolution,projection,resampling])
        print(nodes_to_mod)
        # We need to modify the load_stac process, remove the resample_spatial process, set result=True in load_stac if result_node=True,
        # if not we need to modify the "from_node" of the subsequent process.
        for n in nodes_to_mod:
            jsonGraph["process_graph"][n[1]]["arguments"]["resolution"] = n[3]
            jsonGraph["process_graph"][n[1]]["arguments"]["projection"] = n[4]
            jsonGraph["process_graph"][n[1]]["arguments"]["resampling"] = n[5]
            jsonGraph["process_graph"].pop(n[2])
            if n[0]:
                jsonGraph["process_graph"][n[1]]["result"] = True

            process_graph_string = json.dumps(jsonGraph)
            process_graph_string = process_graph_string.replace(n[2],n[1])
            process_graph = json.loads(process_graph_string)
            pg = OpenEOProcessGraph(process_graph)

        stac_result = pg.to_callable(process_registry.process_registry)()
        if stac_result is not None:
            _log.debug("Returning STAC Collection for the result.")
            return jsonify(stac_result)
        else:
            _log.debug(result_folder_path.split('/')[-1] + '/result'+output_format())
            return jsonify({'output':result_folder_path.split('/')[-1] + '/result'+output_format()})
    except Exception as e:
        _log.error(e)
        return error400('ODC engine error in process: ' + str(e))
    
@app.route('/stop_job', methods=['DELETE'])
def stop_job():
    _log = log_jobid.LogJobID(file=LOG_PATH)
    try:
        job_id = request.args['id']
        _log.job_id(job_id)
        _log.debug('Job id to cancel: {}'.format(job_id))
        if os.path.exists(JOB_LOG_FILE):
            df = pd.read_csv(JOB_LOG_FILE,index_col=0)
            pid = df.loc[df['job_id']==job_id]['pid'].values[0]
            _log.debug('Job PID to stop: {}'.format(pid))
            os.kill(pid, signal.SIGINT)
            df = df[df['job_id']!=job_id]
            df.to_csv(JOB_LOG_FILE)
        return jsonify('ok'), 204
    except Exception as e:
        _log.error(e)
        return error400(str(e))

@app.route('/collections', methods=['GET'])
def list_collections():
    _log = log_jobid.LogJobID(file=LOG_PATH)
    if USE_CACHED_COLLECTIONS:
        if os.path.isfile(METADATA_COLLECTIONS_FILE):
            f = open(METADATA_COLLECTIONS_FILE)
            with open(METADATA_COLLECTIONS_FILE) as collection_list:
                stac_collection = json.load(collection_list)
                return jsonify(stac_collection)
    try:
        collections_list = []
        for cl in EXTERNAL_STAC_COLLECTION:
            collections_list.append(construct_stac_collection(cl))

        collections = {"collections": collections_list}
        with open(METADATA_COLLECTIONS_FILE, 'w') as outfile:
            json.dump(collections, outfile)

        return jsonify(collections)
    except Exception as e:
        _log.error(e)
        return error400(str(e))

@app.route('/processes', methods=['GET'])
def list_processes():
    _log = log_jobid.LogJobID(file=LOG_PATH)
    try:
        if USE_CACHED_PROCESSES:
            if os.path.isfile(METADATA_PROCESSES_FILE):
                f = open(METADATA_PROCESSES_FILE)
                with open(METADATA_PROCESSES_FILE) as processes_list:
                    return jsonify(json.load(processes_list))

        from processing import InitProcesses, output_format
        import openeo_processes_dask

        implemented_processes = []
        processes = InitProcesses(None)
        for p in processes.process_registry:
            implemented_processes.append(p[1])
        json_path = Path(openeo_processes_dask.__file__).parent / "specs" / "openeo-processes"
        process_json_paths = [pg_path for pg_path in (json_path).glob("*.json")]

        # Go through all the jsons in the top-level of the specs folder and add them to be importable from here
        # E.g. from openeo_processes_dask.specs import *
        # This is frowned upon in most python code, but I think here it's fine and allows a nice way of importing these
        implemented_processes_json = []
        for spec_path in process_json_paths:
            spec_json = json.load(open(spec_path))
            process_name = spec_json["id"]
            if process_name in implemented_processes:
                implemented_processes_json.append(spec_json)
        implemented_processes_json = {"processes": implemented_processes_json}
        try:
            with open(METADATA_PROCESSES_FILE, 'w') as outfile:
                json.dump(implemented_processes_json, outfile)
        except:
            pass
        return jsonify(implemented_processes_json)
    except Exception as e:
        _log.error(e)
        return error400(str(e))

@app.route("/collections/<string:name>", methods=['GET'])
def describe_collection(name):
    if not os.path.exists(METADATA_CACHE_FOLDER):
        os.mkdir(METADATA_CACHE_FOLDER)
    if USE_CACHED_COLLECTIONS:
        if os.path.isfile(METADATA_CACHE_FOLDER + '/' + name + '.json'):
            f = open(METADATA_CACHE_FOLDER + '/' + name + '.json')
            with open(METADATA_CACHE_FOLDER + '/' + name + '.json') as collection:
                stacCollection = json.load(collection)
                return jsonify(stacCollection)

    stacCollection = construct_stac_collection(name)

    return jsonify(stacCollection)

def construct_stac_collection(collection_name):
    _log = log_jobid.LogJobID(file=LOG_PATH)
    cl = EXTERNAL_STAC_COLLECTION[collection_name]
    stac_obj = pystac.read_file(cl["href"])
    _log.debug("Constructing the metadata for {}".format(collection_name))
    if not os.path.exists(METADATA_CACHE_FOLDER):
        os.mkdir(METADATA_CACHE_FOLDER)
    if USE_CACHED_COLLECTIONS:
        if os.path.isfile(METADATA_CACHE_FOLDER + '/' + collection_name + '.json'):
            f = open(METADATA_CACHE_FOLDER + '/' + collection_name + '.json')
            with open(METADATA_CACHE_FOLDER + '/' + collection_name + '.json') as collection:
                stac_collection = json.load(collection)
                return stac_collection

    from pystac.extensions.datacube import DatacubeExtension, SpatialDimension, TemporalDimension, HorizontalSpatialDimension, VerticalSpatialDimension, Dimension
    # Set the ID from config
    stac_obj.id = collection_name
    temporal_dims = []
    spatial_dims = []
    bands_dim = []
    if stac_obj.ext.has("cube"):
        if hasattr(stac_obj.ext, "cube"):
            temporal_dims = [
                        (n, d.extent or [None, None])
                        for (n, d) in stac_obj.ext.cube.dimensions.items()
                        if d.dim_type == pystac.extensions.datacube.DimensionType.TEMPORAL
                    ]
            spatial_dims = [
                            (n, d.extent or [None, None])
                            for (n, d) in stac_obj.ext.cube.dimensions.items()
                            if d.dim_type == pystac.extensions.datacube.DimensionType.SPATIAL
                        ]
            bands_dim = [
                            (n, d.values or [None, None])
                            for (n, d) in stac_obj.ext.cube.dimensions.items()
                            if d.dim_type == "bands"
                        ]
    else:
        # Add the datacube extension
        # Extract the temporal extent from the collection
        temporal_extent = stac_obj.extent.temporal.intervals[0]  # First interval (start and end dates)
        temporal_extent = [str(t) for t in temporal_extent]

        # Extract the spatial extent from the collection (bbox coordinates)
        spatial_extent = stac_obj.extent.spatial.bboxes[0]  # First bbox (lon_min, lat_min, lon_max, lat_max)

        # Define the spatial dimensions: lat and lon
        lat_extent = [spatial_extent[1], spatial_extent[3]]  # lat_min, lat_max
        lon_extent = [spatial_extent[0], spatial_extent[2]]  # lon_min, lon_max

        # Add the temporal dimension using the Datacube extension
        datacube = DatacubeExtension.ext(stac_obj, add_if_missing=True)

        # Define the temporal, lat, and lon dimensions
        temporal_dimension = TemporalDimension(
            dict(type=pystac.extensions.datacube.DimensionType.TEMPORAL,
                 extent=temporal_extent)  # Use the extracted temporal extent
        )
        lat_dimension = VerticalSpatialDimension(
            dict(type=pystac.extensions.datacube.DimensionType.SPATIAL,
                 axis="y",
                 extent=lat_extent)  # Use the latitude extent (lat_min, lat_max)
        )
        lon_dimension = HorizontalSpatialDimension(
            dict(type=pystac.extensions.datacube.DimensionType.SPATIAL,
                 axis="x",
                 extent=lon_extent)  # Use the longitude extent (lon_min, lon_max)
        )
        bands_dimension = Dimension(
            dict(type="bands",values=cl["assets"])
        )

        # Assign the dimensions to the datacube
        datacube.dimensions = {
            DEFAULT_TEMPORAL_DIMENSION_NAME: temporal_dimension,
            DEFAULT_Y_DIMENSION_NAME: lat_dimension,
            DEFAULT_X_DIMENSION_NAME: lon_dimension,
            DEFAULT_BANDS_DIMENSION_NAME: bands_dimension,
        }

    datacube = DatacubeExtension.ext(stac_obj)
    current_dims = datacube.dimensions
    if DEFAULT_BANDS_DIMENSION_NAME not in current_dims:
        current_dims[DEFAULT_BANDS_DIMENSION_NAME] = Dimension(
            dict(type="bands",values=cl["assets"])
        )
        datacube.dimensions = current_dims
    with open(METADATA_CACHE_FOLDER + '/' + collection_name + '.json', 'w') as outfile:
        json.dump(stac_obj.to_dict(), outfile)
    return stac_obj.to_dict()
