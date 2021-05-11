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

DATACUBE_EXPLORER_ENDPOINT = "http://0.0.0.0:9000"
OPENDATACUBE_CONFIG_FILE = ""
USE_CACHED_COLLECTIONS = True
ODC_COLLECTIONS_FILE = "ODC_collections.json"

def sar2cube_collection_extent(collectionName):
    dc = datacube.Datacube(config = OPENDATACUBE_CONFIG_FILE)
    sar2cubeData = dc.load(product = collectionName, dask_chunks={'time':1,'x':2000,'y':2000})
    zero_lon_mask = sar2cubeData.grid_lon[0]>0
    zero_lat_mask = sar2cubeData.grid_lat[0]>0
    min_lon = sar2cubeData.grid_lon[0].where(zero_lon_mask).min().values.item(0)
    min_lat = sar2cubeData.grid_lat[0].where(zero_lat_mask).min().values.item(0)
    max_lon = sar2cubeData.grid_lon[0].max().values.item(0)
    max_lat = sar2cubeData.grid_lat[0].max().values.item(0)
    return [min_lon,min_lat,max_lon,max_lat]

app = Flask('openeo_odc_driver')

@app.errorhandler(500)
def error500(error):
    return error, 500 

@app.route('/graph', methods=['POST'])
def process_graph():
    jsonGraph = request.json
    try:
        eo = OpenEO(jsonGraph,0)
        return send_file(eo.tmpFolderPath + "/output"+eo.outFormat, as_attachment=True, attachment_filename='output'+eo.outFormat)
    except Exception as e:
        return error500("ODC back-end failed processing! \n" + str(e))

@app.route('/collections', methods=['GET'])
def list_collections():
    if USE_CACHED_COLLECTIONS:
        try:
            f = open(ODC_COLLECTIONS_FILE)
            with open(ODC_COLLECTIONS_FILE) as collection_list:
                stacCollection = json.load(collection_list)
                return jsonify(stacCollection)
        except IOError:
            pass
            
    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/products.txt")
    datacubesList = res.text.split('\n')
    collections = {}
    collections['collections'] = {}
    for i,d in enumerate(datacubesList):
        ## TODO: if SAR2Cube datacube, we need to read the coverage from somewhere else
        res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/collections/" + d)
        stacCollection = res.json()
        stacCollection.pop('properties')
        stacCollection['license'] = 'CC-BY-4.0'
        stacCollection['providers'] = [{'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['producer','host']}]
        stacCollection['links'] = {}
        stacCollection['links'] = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
        collections['collections'][i] = stacCollection
        if "SAR2Cube" in d:
            try:
                sar2cubeBbox = sar2cube_collection_extent(d)
                stacCollection['extent']['spatial']['bbox'] = [sar2cubeBbox]
            except:
                pass
    with open('ODC_collections.json', 'w') as outfile:
        json.dump(collections, outfile)
        
    return jsonify(collections)


@app.route("/collections/<string:name>", methods=['GET'])
def describe_collection(name):
    if USE_CACHED_COLLECTIONS:
        try:
            f = open(name + '.json')
            # Do something with the file
            with open(name + '.json') as collection:
                stacCollection = json.load(collection)
                return jsonify(stacCollection)
        except IOError:
            pass
        
    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/collections/" + name)
    stacCollection = res.json()
    stacCollection.pop('properties')
    stacCollection['license'] = 'CC-BY-4.0'
    stacCollection['providers'] = [{'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['producer','host']}]
    stacCollection['links'] = {}
    stacCollection['links'] = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
    stacCollection['stac_extensions'] = ['datacube']
    stacCollection['stac_extensions'] = ['datacube']
    stacCollection['cube:dimensions'] = {}
    stacCollection['cube:dimensions']['DATE'] = {}
    stacCollection['cube:dimensions']['DATE']['type'] = 'temporal'
    stacCollection['cube:dimensions']['DATE']['extent'] = stacCollection['extent']['temporal']['interval'][0]
    
    stacCollection['cube:dimensions']['X'] = {}
    stacCollection['cube:dimensions']['X']['type'] = 'spatial'
    stacCollection['cube:dimensions']['X']['axis'] = 'x'
    stacCollection['cube:dimensions']['X']['extent'] = [stacCollection['extent']['spatial']['bbox'][0][0],stacCollection['extent']['spatial']['bbox'][0][2]]  
    stacCollection['cube:dimensions']['X']['reference_system'] = 'unknown'

    stacCollection['cube:dimensions']['Y'] = {}
    stacCollection['cube:dimensions']['Y']['type'] = 'spatial'
    stacCollection['cube:dimensions']['Y']['axis'] = 'y'
    stacCollection['cube:dimensions']['Y']['extent'] = [stacCollection['extent']['spatial']['bbox'][0][1],stacCollection['extent']['spatial']['bbox'][0][3]]
    stacCollection['cube:dimensions']['Y']['reference_system'] = 'unknown'
    
    res = requests.get(DATACUBE_EXPLORER_ENDPOINT + "/collections/" + name + "/items")
    items = res.json()
    yamlFile = items['features'][0]['assets']['location']['href']
    yamlFile = yamlFile.split('file://')[1].replace('%40','@')
    
    with open(yamlFile, 'r') as stream:
        try:
            yamlDATA = yaml.safe_load(stream)
            stacCollection['cube:dimensions']['X']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
            stacCollection['cube:dimensions']['Y']['reference_system'] = int(yamlDATA['grid_spatial']['projection']['spatial_reference'].split('EPSG')[-1].split('\"')[-2])
        except yaml.YAMLError as exc:
            print(exc)
    
    keys = items['features'][0]['assets'].keys()
    list_keys = list(keys)
    list_keys.remove('location')
    bands_list = []
    for key in list_keys:
        if len(items['features'][0]['assets'][key]['eo:bands'])>1:
            for b in items['features'][0]['assets'][key]['eo:bands']:
                bands_list.append(b)
        else:
            bands_list.append(items['features'][0]['assets'][key]['eo:bands'][0])
            
    stacCollection['cube:dimensions']['bands'] = {}
    stacCollection['cube:dimensions']['bands']['type'] = 'bands'
    stacCollection['cube:dimensions']['bands']['values'] = bands_list
    summaries_dict = {}
    summaries_dict['constellation'] = ['No Constellation Information Available']
    summaries_dict['platform']      = ['No Platform Information Available']
    summaries_dict['instruments']   = ['No Instrument Information Available']
    cloud_cover_dict = {}
    cloud_cover_dict['min'] = 0
    cloud_cover_dict['max'] = 0
    summaries_dict['eo:cloud cover'] = cloud_cover_dict
    summaries_dict['eo:gsd'] = [0]
    eo_bands_list_dict = []
    for band in bands_list:
        eo_bands_dict = {}
        eo_bands_dict['name'] = band
        eo_bands_dict['common_name'] = band
        eo_bands_dict['center_wavelength'] = 0
        eo_bands_dict['gsd'] = 0
        eo_bands_list_dict.append(eo_bands_dict)
        
    summaries_dict['eo:bands'] = eo_bands_list_dict
    stacCollection['summaries'] = summaries_dict
    if "SAR2Cube" in name:
        try:
            sar2cubeBbox = sar2cube_collection_extent(name)
            stacCollection['extent']['spatial']['bbox'] = [sar2cubeBbox]
            stacCollection['cube:dimensions']['X']['extent'] = [sar2cubeBbox[0],sar2cubeBbox[2]]  
            stacCollection['cube:dimensions']['Y']['extent'] = [sar2cubeBbox[1],sar2cubeBbox[3]]
        except:
            pass
    with open(name + '.json', 'w') as outfile:
        json.dump(stacCollection, outfile)
        
    return jsonify(stacCollection)
