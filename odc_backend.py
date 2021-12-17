# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   11/05/2021

import dask
from dask.distributed import Client
from openeo_odc_driver import OpenEO
import argparse
import os
import sys
from flask import Flask, request, jsonify, send_file
import json
import requests
import yaml
import datacube
from config import *
import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("sar2cube_debug.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
def sar2cube_collection_extent(collectionName):
    dc = datacube.Datacube(config = OPENDATACUBE_CONFIG_FILE)
    sar2cubeData = dc.load(product = collectionName, dask_chunks={'time':1,'x':2000,'y':2000})
    zero_lon_mask = sar2cubeData.grid_lon[0]!=0
    zero_lat_mask = sar2cubeData.grid_lat[0]!=0
    min_lon = sar2cubeData.grid_lon[0].where(zero_lon_mask).min().values.item(0)
    min_lat = sar2cubeData.grid_lat[0].where(zero_lat_mask).min().values.item(0)
    max_lon = sar2cubeData.grid_lon[0].where(zero_lon_mask).max().values.item(0)
    max_lat = sar2cubeData.grid_lat[0].where(zero_lat_mask).max().values.item(0)
    return [min_lon,min_lat,max_lon,max_lat]

app = Flask('openeo_odc_driver')

@app.errorhandler(500)
def error500(error):
    return error, 500 

@app.route('/graph', methods=['POST'])
def process_graph():
    jsonGraph = request.json
    try:
        eo = OpenEO(jsonGraph)
        if eo.returnFile:
            return send_file(eo.tmpFolderPath + "/output"+eo.outFormat, as_attachment=True, attachment_filename='output'+eo.outFormat)
            print("OUTPUT FILE: ",eo.tmpFolderPath + "/output"+eo.outFormat)
        else:
            return jsonify({"output":eo.tmpFolderPath})
            print("OUTPUT FILE: ",eo.tmpFolderPath)

    except Exception as e:
        return error500("ODC engine error in process: " + str(e))

@app.route('/collections', methods=['GET'])
def list_collections():
    if USE_CACHED_COLLECTIONS:
        if os.path.isfile(ODC_COLLECTIONS_FILE):
            f = open(ODC_COLLECTIONS_FILE)
            with open(ODC_COLLECTIONS_FILE) as collection_list:
                stacCollection = json.load(collection_list)
                return jsonify(stacCollection)
    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/products.txt")
    datacubesList = res.text.split('\n')
    collections = {}
    collections['collections'] = []
    collectionsList = []
    for i,d in enumerate(datacubesList):
        currentCollection = construct_stac_collection(d)
        collectionsList.append(currentCollection)
    collections['collections'] = collectionsList
    with open(ODC_COLLECTIONS_FILE, 'w') as outfile:
        json.dump(collections, outfile)
    return jsonify(collections)


@app.route("/collections/<string:name>", methods=['GET'])
def describe_collection(name):
    if USE_CACHED_COLLECTIONS:
        if os.path.isfile(METADATA_FOLDER + '/CACHE/' + name + '.json'):
            f = open(METADATA_FOLDER + '/CACHE/' + name + '.json')
            with open(METADATA_FOLDER + '/CACHE/' + name + '.json') as collection:
                stacCollection = json.load(collection)
                return jsonify(stacCollection)

    stacCollection = construct_stac_collection(name)
        
    return jsonify(stacCollection)

def construct_stac_collection(collectionName):
    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/collections/" + collectionName)
    stacCollection = res.json()
    metadata = None
    if os.path.isfile(METADATA_FOLDER + '/SUPP/' + collectionName + '_supp_metadata.json'):
        additional_metadata = open(METADATA_FOLDER + '/SUPP/' + collectionName + '_supp_metadata.json')
        metadata = json.load(additional_metadata)

    stacCollection['stac_extensions'] = ['datacube']         
    stacCollection.pop('properties')
    stacCollection['license'] = 'CC-BY-4.0'
    stacCollection['providers'] = [{'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['producer','host']}]
    stacCollection['links'] = {}
    stacCollection['links'] = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
    if "SAR2Cube" in collectionName:
        try:
            sar2cubeBbox = sar2cube_collection_extent(collectionName)
            stacCollection['extent']['spatial']['bbox'] = [sar2cubeBbox]
        except Exception as e:
            logging.error(e)
            pass

    ### SUPPLEMENTARY METADATA FROM FILE
    if metadata is not None:
        stacCollection['title']       = metadata['title']
        stacCollection['description'] = metadata['description']
        if 'keywords' in metadata.keys():
            stacCollection['keywords']     = metadata['keywords']
        if 'providers' in metadata.keys():
            stacCollection['providers']    = metadata['providers']
        if 'version' in metadata.keys():
            stacCollection['version']      = metadata['version']
        if 'deprecated' in metadata.keys():
            stacCollection['deprecated']   = metadata['deprecated']
        if 'license' in metadata.keys():
            stacCollection['license']      = metadata['license']
        if 'sci:citation' in metadata.keys():
            stacCollection['sci:citation'] = metadata['sci:citation']
            stacCollection['stac_extensions'] = ['datacube','scientific']
        if 'links' in metadata.keys():
            stacCollection['links']        = metadata['links']
        if 'summaries' in metadata.keys():
            stacCollection['summaries'] = {}
            if 'rows' in metadata['summaries']:
                stacCollection['summaries']['rows']           = metadata['summaries']['rows']
            if 'columns' in metadata['summaries']:
                stacCollection['summaries']['columns']        = metadata['summaries']['columns']
            if 'gsd' in metadata['summaries']:
                stacCollection['summaries']['gsd']            = metadata['summaries']['gsd']
            if 'constellation' in metadata['summaries']:
                stacCollection['summaries']['constellation']  = metadata['summaries']['constellation']
            if 'platform' in metadata['summaries']:
                stacCollection['summaries']['platform']       = metadata['summaries']['platform']
            if 'instruments' in metadata['summaries']:
                stacCollection['summaries']['instruments']    = metadata['summaries']['instruments']
            if 'eo:cloud cover' in metadata['summaries']:
                stacCollection['summaries']['eo:cloud cover'] = metadata['summaries']['eo:cloud cover']
        if 'cube:dimensions' in metadata.keys():
            if 'bands' in metadata['cube:dimensions'].keys():
                if 'values' in metadata['cube:dimensions']['bands'].keys():
                    stacCollection['cube:dimensions']['bands'] = {}
                    stacCollection['cube:dimensions']['bands']['type'] = 'bands'
                    stacCollection['cube:dimensions']['bands']['values'] = metadata['cube:dimensions']['bands']['values']

    ### SPATIAL AND TEMPORAL EXTENT FROM DATACUBE-EXPLORER
    stacCollection['cube:dimensions'] = {}
    stacCollection['cube:dimensions']['DATE'] = {}
    stacCollection['cube:dimensions']['DATE']['type'] = 'temporal'
    stacCollection['cube:dimensions']['DATE']['extent'] = stacCollection['extent']['temporal']['interval'][0]

    stacCollection['cube:dimensions']['X'] = {}
    stacCollection['cube:dimensions']['X']['type'] = 'spatial'
    stacCollection['cube:dimensions']['X']['axis'] = 'x'
    stacCollection['cube:dimensions']['X']['extent'] = [stacCollection['extent']['spatial']['bbox'][0][0],stacCollection['extent']['spatial']['bbox'][0][2]]

    stacCollection['cube:dimensions']['Y'] = {}
    stacCollection['cube:dimensions']['Y']['type'] = 'spatial'
    stacCollection['cube:dimensions']['Y']['axis'] = 'y'
    stacCollection['cube:dimensions']['Y']['extent'] = [stacCollection['extent']['spatial']['bbox'][0][1],stacCollection['extent']['spatial']['bbox'][0][3]]

    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/collections/" + collectionName + "/items")
    items = res.json()

    ## TODO: remove this part when all the datacubes have a metadata file, crs comes from there
    try:
        yamlFile = items['features'][0]['assets']['location']['href']
        yamlFile = yamlFile.split('file://')[1].replace('%40','@').replace('%3A',':')

        with open(yamlFile, 'r') as stream:
            try:
                yamlDATA = yaml.safe_load(stream)
                stacCollection['cube:dimensions']['X']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
                stacCollection['cube:dimensions']['Y']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
            except Exception as e:
                print(e)
    except:
        pass

    if metadata is not None:
        if 'crs' in metadata.keys():
            stacCollection['cube:dimensions']['X']['reference_system'] = metadata['crs']
            stacCollection['cube:dimensions']['Y']['reference_system'] = metadata['crs']
    
    ### BANDS FROM DATACUBE-EXPLORER IF NOT ALREADY PROVIDED IN THE SUPP METADATA
    bands_list = []
    try:
        keys = items['features'][0]['assets'].keys()
        list_keys = list(keys)
        list_keys.remove('location')
        try:
            for key in list_keys:
                if len(items['features'][0]['assets'][key]['eo:bands'])>1:
                    for b in items['features'][0]['assets'][key]['eo:bands']:
                        bands_list.append(b)
                else:
                    bands_list.append(items['features'][0]['assets'][key]['eo:bands'][0])
            stacCollection['cube:dimensions']['bands'] = {}
            stacCollection['cube:dimensions']['bands']['type'] = 'bands'
            stacCollection['cube:dimensions']['bands']['values'] = bands_list
        except Exception as e:
            print(e)
    except Exception as e:
        print(e)

    with open(METADATA_FOLDER + '/CACHE/' + collectionName + '.json', 'w') as outfile:
        json.dump(stacCollection, outfile)
    return stacCollection