import inspect
import logging
from pathlib import Path
from osgeo import gdal
import numpy as np
import openeo_processes_dask.process_implementations
import openeo_processes_dask.specs
import rasterio
import rioxarray
import xarray as xr
import cv2
import os
import pyproj
import requests
import json
from datetime import datetime, timezone
from openeo_pg_parser_networkx import ProcessRegistry
from openeo_pg_parser_networkx.process_registry import Process
from openeo_processes_dask.process_implementations.core import process
from openeo_processes_dask.process_implementations.data_model import RasterCube
from load_odc_collection import LoadOdcCollection

import log_jobid
from config import *

global RESULT_FOLDER
global OUTPUT_FORMAT
global JOB_ID
global IS_BATCH_JOB

def output_format():
    global OUTPUT_FORMAT
    return OUTPUT_FORMAT

class JobId():
    def __init__(self,job_id):
        global JOB_ID
        JOB_ID = job_id

def get_job_id():
    global JOB_ID
    return JOB_ID

def load_collection(*args, **kwargs):

    global JOB_ID

    _log = log_jobid.LogJobID(file=LOG_PATH)
    _log.set_job_id(JOB_ID)

    from openeo_processes_dask.process_implementations.cubes._filter import (
        _reproject_bbox,
    )
    default_time_start = '1970-01-01'
    default_time_end   = str(datetime.now()).split(' ')[0] # Today is the default date for default_time_end, to include all the dates if not specified
    time_start         = default_time_start
    time_end           = default_time_end
    collection         = None
    south              = None
    north              = None
    east               = None
    west               = None
    bands              = None # List of bands
    resolutions        = None # Tuple
    output_crs         = None
    crs                = None
    resampling_method  = None
    polygon            = None
    if 'bands' in kwargs:
        bands = kwargs['bands']
        if bands == []: bands = None

    collection = kwargs['id'] # The datacube we have to load
    if collection is None:
        raise Exception('[!] You must provide a collection name!')
    # self.sar2cubeCollection = ('SAR2Cube' in collection) # Return True if it's a SAR2Cube collection

    if kwargs['temporal_extent'] is not None:
        temporal_extent = kwargs['temporal_extent']
        if temporal_extent[0] is not None:
            time_start = str(temporal_extent[0].to_numpy())
        if temporal_extent[1] is not None:
            time_end = str(temporal_extent[1].to_numpy())
    # If there is a bounding-box or a polygon we set the variables, otherwise we pass the defaults
    if 'spatial_extent' in kwargs and kwargs['spatial_extent'] is not None:
        spatial_extent = kwargs['spatial_extent']
        try:
            spatial_extent_4326 = spatial_extent
            if spatial_extent.crs is not None:
                if not pyproj.crs.CRS(spatial_extent.crs).equals("EPSG:4326"):
                    spatial_extent_4326 = _reproject_bbox(
                        spatial_extent, "EPSG:4326"
                    )
            west = spatial_extent_4326.west
            south = spatial_extent_4326.south
            east = spatial_extent_4326.east
            north = spatial_extent_4326.north
        except Exception as e:
            raise Exception(f"Unable to parse the provided spatial extent: {e}")

    elif 'coordinates' in kwargs['spatial_extent']:
        # Pass coordinates to odc and process them there
        polygon = kwargs['spatial_extent']['coordinates']

    if 'crs' in kwargs['spatial_extent'] and kwargs['spatial_extent']['crs'] is not None:
        crs = kwargs['spatial_extent']['crs']

    odc_collection = LoadOdcCollection(collection_id=collection,
                                        time_start=time_start,
                                        time_end=time_end,
                                        bands=bands,
                                        south=south,
                                        north=north,
                                        west=west,
                                        east=east,
                                        resolutions=resolutions,
                                        output_crs=output_crs,
                                        polygon=polygon,
                                        resampling_method=resampling_method,
                                        crs=crs,
                                        job_id=JOB_ID)
    if len(odc_collection.data) == 0:
        raise Exception("load_collection returned an empty dataset, please check the requested bands, spatial and temporal extent.")
    data = odc_collection.data.to_array(dim="bands")
    # self.crs = odc_collection.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
    _log.debug(data) # The loaded data, stored in a dictionary with the id of the node that has generated it
    return data

def save_result(*args, **kwargs):
    global RESULT_FOLDER
    global OUTPUT_FORMAT
    global JOB_ID
    global IS_BATCH_JOB

    _log = log_jobid.LogJobID(file=LOG_PATH)
    _log.set_job_id(JOB_ID)

    _log.debug(f"Result folder: {RESULT_FOLDER}")
    pretty_args = {k: repr(v)[:80] for k, v in kwargs.items()}
    _log.debug(
            f"Running process save_result with resolved parameters: {pretty_args}"
        )
    data = kwargs['data']
    out_format = kwargs['format']
    if out_format.lower() == 'png':
        OUTPUT_FORMAT = '.png'
        # mimeType = 'image/png'

        data = data.fillna(0)

        # This is required as a workaround to this issue: https://github.com/Open-EO/openeo-web-editor/issues/280
        ### Start of workaround
        if 'y' in data.dims:
            if len(data.y)>1:
                if data.y[0] < data.y[-1]:
                    data = data.isel(y=slice(None, None, -1))
        ### End of workaround
        size = None; red = None; green = None; blue = None; gray = None
        if 'options' in kwargs:
            if 'size' in kwargs['options']:
                size = kwargs['options']['size']
            if 'red' in kwargs['options']:
                red = kwargs['options']['red']
            if 'green' in kwargs['options']:
                green = kwargs['options']['green']
            if 'blue' in kwargs['options']:
                blue = kwargs['options']['blue']
            if 'gray' in kwargs['options']:
                gray = kwargs['options']['gray']
            if red is not None and green is not None and blue is not None and gray is not None:
                redBand   = data.loc[dict(variable=red)].values
                blueBand  = data.loc[dict(variable=blue)].values
                greenBand = data.loc[dict(variable=green)].values
                grayBand  = data.loc[dict(variable=gray)].values
                bgr = np.stack((blueBand,greenBand,redBand,grayBand),axis=2)
            elif red is not None and green is not None and blue is not None:
                redBand   = data.loc[dict(variable=red)].values
                blueBand  = data.loc[dict(variable=blue)].values
                greenBand = data.loc[dict(variable=green)].values
                bgr = np.stack((blueBand,greenBand,redBand),axis=2)
            else:
                bgr = data.values
                if bgr.shape[0] in [1,2,3,4]:
                    bgr = np.moveaxis(bgr,0,-1)
        else:
            bgr = data.values
            if bgr.shape[0] in [1,2,3,4]:
                bgr = np.moveaxis(bgr,0,-1)
        if size is not None: # The OpenEO API let the user set the "longest dimension of the image in pixels"
            # 1 find the bigger dimension
            if bgr.shape[0] > bgr.shape[1]:
                scaleFactor = size/bgr.shape[0]
                width = int(bgr.shape[1] * scaleFactor)
                height = int(size)
                dsize = (width, height)
                # 2 resize
                bgr = cv2.resize(bgr, dsize)
            else:
                scaleFactor = size/bgr.shape[1]
                width = int(size)
                height = int(bgr.shape[0] * scaleFactor)
                dsize = (width, height)
                bgr = cv2.resize(bgr, dsize)
        bgr = bgr.astype(np.uint8)
        cv2.imwrite(RESULT_FOLDER + '/result.png',bgr)
        return

    if out_format.lower() in ['gtiff','geotiff','tif','tiff']:
        # If batch job: return STAC Collection
        # If not (sync call) return single file
        if IS_BATCH_JOB:
            from raster2stac import Raster2STAC
            job_id = get_job_id()
            # To create valid STAC Items we need a datetime associated with the data. If it has been reduced, like when using reduce_dimension, we need to get it back
            if len(data.openeo.temporal_dims)==0:
                t_dim = "time"
                t_value = data.attrs.get("reduced_dimensions_min_values",None).get(t_dim,None)
                if t_value is None:
                    t_value = np.datetime64('now')
                data = data.expand_dims(dim={t_dim: [t_value]}, axis=0)
            rs2stac = Raster2STAC(
                data = data, # The input xarray object which will be converted into COGs
                collection_id = job_id, # The Collection id we want to set
                description = f"openEO results for the job with id {job_id}",
                collection_url = STAC_API_URL, # The URL where the collection will be exposed
                output_folder = RESULT_FOLDER,
                bucket_file_prefix = "OPENEO_RESULT/",
                s3_upload = True,
                bucket_name = "eurac-eo",
                aws_access_key = os.environ.get("AWS_ACCESS_KEY"),
                aws_secret_key = os.environ.get("AWS_SECRET_KEY"),
                aws_region = "s3-eu-west-1",
                # If this runs in a Docker, the files path will be the ones of the Docker internal structure. It's necessary to change the paths to the external folder mounted by Docker so that the files referred in the STAC document can also be accessble from outside the Docker (but with data access to the drive)
                # /mnt/large_drive/work_spaces/mclaus/data/odc-driver/:/data/odc-driver/
                write_collection_assets=True
            ).generate_cog_stac()
            # Now we should have the resulting COGs and STAC json STAC files. We can POST them to the STAC Catalog

            with open(f"{RESULT_FOLDER}/{job_id}.json","r") as f:
                stac_collection_to_post = json.load(f)
            with open(f"{RESULT_FOLDER}/process_graph.json","r") as pg:
                process_graph = json.load(pg)
            # TODO: add process graph to STAC
            # TODO: also add processing extension

            local_time = datetime.now(timezone.utc).astimezone()
            time_string = local_time.isoformat()
            EURAC_RESEARCH_PROVIDER = {
                                      "name": "Eurac Research - Institute for Earth Observation",
                                      "url": "http://www.eurac.edu",
                                      "roles": [
                                        "processor"
                                      ],
                                      "description": "This data was processed on an openEO backend maintained by Eurac Research.",
                                      "processing:facility": "Eurac Research openEO backend",
                                      "processing:software": {
                                        "openeo-spring-driver": "1.2.0",
                                        "openeo_odc_driver": "0.0.1",
                                      },
                                      "processing:expression": {
                                        "format": "openeo",
                                        "expression": process_graph,
                                      },
                                      "processing:datetime": time_string,
                                    }
            stac_collection_to_post["providers"] = [EURAC_RESEARCH_PROVIDER]

            if POST_RESULTS_TO_STAC:
                requests.post(STAC_API_URL,json=stac_collection_to_post)
                stac_items = []
                with open(f"{RESULT_FOLDER}/inline_items.csv","r") as f:
                    stac_items = f.readlines()
                for it in stac_items:
                    stac_data_to_post = json.loads(it)
                    requests.post(f"{STAC_API_URL}{job_id}/items",json=stac_data_to_post)
            return stac_collection_to_post
        else:
            OUTPUT_FORMAT = '.tiff'
            if data.dtype == 'bool':
                data = data.astype(np.uint8)
            band_dims = None
            time_dim = None
            if data.openeo.band_dims is not None  and len(data.openeo.band_dims) > 0:
                band_dim = data.openeo.band_dims[0]
            if data.openeo.temporal_dims is not None and len(data.openeo.temporal_dims) > 0:
                time_dim = data.openeo.temporal_dims[0]
            if len(data.dims) > 3:
                if len(data[time_dim])>=1 and len(data[band_dim])==1:
                    # We keep the time dimension as band in the GeoTiff, timeseries of a single band/variable
                    data = data.squeeze(band_dims).to_dataset(name='result')
                elif len(data[time_dim])==1 and len(data[band_dim])>=1:
                    # We keep the time variable as band in the GeoTiff, multiple band/variables of the same timestamp
                    data = data.drop([time_dim]).squeeze([time_dim])
                else:
                    raise Exception("[!] Not possible to write a 4-dimensional GeoTiff, use NetCDF instead.")
            else:
                data = data 

            # This is required as a workaround to this issue: https://github.com/Open-EO/openeo-web-editor/issues/280
            ### Start of workaround
            if 'y' in data.dims:
                if len(data.y)>1:
                    if data.y[0] < data.y[-1]:
                        data = data.isel(y=slice(None, None, -1))
            ### End of workaround
            # data.attrs['crs'] = self.crs
            # if band_dim is not None:
                # data = data.to_dataset(dim=band_dim)
            # data.rio.to_raster(self.result_folder_path + "/result.tiff")
            # ds = gdal.Open(self.result_folder_path + "/result.tiff", gdal.GA_Update)
            data.rio.to_raster(RESULT_FOLDER + "/result.tiff")
            ds = gdal.Open(RESULT_FOLDER + "/result.tiff", gdal.GA_Update)
            n_of_bands = ds.RasterCount
            for band in range(n_of_bands):
                ds.GetRasterBand(band+1).ComputeStatistics(0)
                ds.GetRasterBand(band+1).SetNoDataValue(np.nan)

        return

    if out_format.lower() in ['netcdf','nc']:
        OUTPUT_FORMAT = '.nc'
        _log.debug(kwargs)
        try:
            data.to_netcdf(RESULT_FOLDER + "/result.nc")
            return
        except Exception as e:
            _log.info(e)
            _log.info("Wrtiting netcdf failed, trying another time....")
            pass
        try:
            if 'units' in data.time.attrs:
                data.time.attrs.pop('units', None) #TODO: use .openeo to get temporal dims
            data.to_netcdf(RESULT_FOLDER + "/result.nc")
        except Exception as e:
            _log.info(e)
            _log.info("Wrtiting netcdf failed, trying another time....")
            pass
        try:
            # Remove problematic attributes and coordinates, which prevent to write a valid netCDF file
            for at in data.attrs:
                # allowed types: str, Number, ndarray, number, list, tuple
                if not isinstance(data.attrs[at], (int, float, str, np.ndarray, list, tuple)):
                    data.attrs[at] = str(data.attrs[at])

            for c in data.coords:
                if data[c].dtype=="object":
                    data = data.drop_vars(c)            

            data.to_netcdf(RESULT_FOLDER + "/result.nc")
        except Exception as e:
            _log.error(e)
            _log.error("Wrtiting netcdf failed!")
        return

    if out_format.lower() == 'json':
        self.out_format = '.json'
        self.mimeType = 'application/json'
        if isinstance(data,gpd.geodataframe.GeoDataFrame):
            data.to_file(self.result_folder_path + "/result.json", driver="GeoJSON")
            return
        else:
            data = data.compute()
            band_dim = 'variable'
            dims = list(data.dims)
            if band_dim in dims:
                dims_no_bands = dims.copy()
                dims_no_bands.remove(band_dim)
            else:
                dims_no_bands = dims

            n_dims = len(dims_no_bands)
            data_dict = {}
            if n_dims==0:
                if band_dim in dims:
                    # Return dict with bands as keys
                    for i,b in enumerate(data[band_dim].values):
                        data_dict[b] = [[data.loc[{band_dim:b}].item(0)]]
                else:
                    # This should be a single value
                    data_dict['0'] = [[data.item(0)]]
            elif n_dims==1:
                if band_dim in dims:
                    # Return dict with dimension as key and bands as columns
                    for j in range(len(data[dims_no_bands[0]])):
                        index = str(data[dims_no_bands[0]][j].values)
                        data_list = {}
                        for i,b in enumerate(data[band_dim].values):
                            data_list[b] = [data.loc[{band_dim:b,dims_no_bands[0]:index}].values]
                        data_dict[index] = data_list
                else:
                    # Return dict with dimension as key and value as column
                    for j in range(len(data[dims_no_bands[0]])):
                        index = str(data[dims_no_bands[0]][j].values)
                        data_dict[index] = [[data.loc[{dims_no_bands[0]:index}].values]]
            else:
                data_dict = data.to_dict()
            with open(self.result_folder_path + "/result.json", 'w') as outfile:
                json.dump(data_dict,outfile,default=str)
            return 

    else:
        raise Exception("[!] Output format not recognized/implemented: {0}".format(out_format))

    return

class InitProcesses():
    def __init__(self,result_folder,is_batch_job=False):
        global RESULT_FOLDER
        global IS_BATCH_JOB
        RESULT_FOLDER = result_folder
        IS_BATCH_JOB = is_batch_job
        self.process_registry = None
        self.init_process_registry()
        
    def init_process_registry(self):
        self.process_registry = ProcessRegistry(wrap_funcs=[process])

        # Import these pre-defined processes from openeo_processes_dask and register them into registry
        processes_from_module = [
            func
            for _, func in inspect.getmembers(
                openeo_processes_dask.process_implementations,
                inspect.isfunction,
            )
        ]

        specs = {}
        for func in processes_from_module:
            try:
                specs[func.__name__] = getattr(openeo_processes_dask.specs, func.__name__)
            except Exception:
                continue

        for func in processes_from_module:
            try:
                self.process_registry[func.__name__] = Process(
                spec=specs[func.__name__], implementation=func
                )
            except Exception:
                continue

        self.process_registry["save_result"] = Process(
        spec=openeo_processes_dask.specs.save_result,
        implementation=save_result,
        )
        self.process_registry["load_collection"] = Process(
        spec=openeo_processes_dask.specs.load_collection,
        implementation=load_collection,
        )
