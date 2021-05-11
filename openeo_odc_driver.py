# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   10/02/2021

# Import necessary libraries
# System
import os
import sys
from time import time
from datetime import datetime
import json
import uuid
# Math
import math
import numpy as np
from scipy.interpolate import griddata
from scipy.spatial import Delaunay
from scipy.interpolate import LinearNDInterpolator
# Geography
from osgeo import gdal, osr
from pyproj import Proj, transform, Transformer, CRS
import rasterio
import rasterio.features
# Datacubes and databases
import datacube
import xarray as xr
import rioxarray
import pandas as pd
# Parallel Computing
import dask
from dask.distributed import Client
from dask import delayed
# openEO & SAR2Cube specific
from openeo_pg_parser.translate import translate_process_graph
from odc_wrapper import Odc
try:
    from sar2cube_utils import *
except:
    pass

DASK_SCHEDULER_ADDRESS = ''
TMP_FOLDER_PATH        = '' # Has to be accessible from all the Dask workers
client = Client(DASK_SCHEDULER_ADDRESS)


class OpenEO():
    def __init__(self,jsonProcessGraph):
        self.jsonProcessGraph = jsonProcessGraph
        self.data = None
        self.listExecutedIds = []
        self.partialResults = {}
        self.crs = None
        self.bands = None
        self.graph = translate_process_graph(jsonProcessGraph).sort(by='dependency')
        self.outFormat = None
        self.mimeType = None
        self.i = 0
        self.tmpFolderPath = TMP_FOLDER_PATH + str(uuid.uuid4())
        try:
            os.mkdir(self.tmpFolderPath)
        except:
            pass
        for i in range(0,len(self.graph)+1):
            if not self.process_node(i):
                print('[*] Processing finished!')
                break

    def process_node(self,i):
        node = self.graph[i]
        processName = node.process_id
        print("Process id: {} Process name: {}".format(node.id,processName))
        try:
            if processName == 'load_collection':
                defaultTimeStart = '1970-01-01'
                defaultTimeEnd   = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
                timeStart        = defaultTimeStart
                timeEnd          = defaultTimeEnd
                collection       = None
                lowLat           = None
                highLat          = None
                lowLon           = None
                highLon          = None
                bands            = None # List of bands
                resolutions      = None # Tuple
                outputCrs        = None
                resamplingMethod = None
                polygon          = None
                if 'bands' in node.arguments:
                    bands = node.arguments['bands']
                    if bands == []: bands = None

                collection = node.arguments['id'] # The datacube we have to load
                if collection is None:
                    raise Exception('[!] You must provide a collection which provides the data!')

                if node.arguments['temporal_extent'] is not None:
                    timeStart  = node.arguments['temporal_extent'][0]
                    timeEnd    = node.arguments['temporal_extent'][1]

                # If there is a bounding-box or a polygon we set the variables, otherwise we pass the defaults
                if 'spatial_extent' in node.arguments:
                    if 'south' in node.arguments['spatial_extent'] and \
                       'north' in node.arguments['spatial_extent'] and \
                       'east'  in node.arguments['spatial_extent'] and \
                       'west'  in node.arguments['spatial_extent']:
                        lowLat     = node.arguments['spatial_extent']['south']
                        highLat    = node.arguments['spatial_extent']['north']
                        lowLon     = node.arguments['spatial_extent']['east']
                        highLon    = node.arguments['spatial_extent']['west']

                    elif 'coordinates' in node.arguments['spatial_extent']:
                        # Pass coordinates to odc and process them there
                        polygon = node.arguments['spatial_extent']['coordinates']

                for n in self.graph: # Let's look for resample_spatial nodes
                    parentID = 0
                    if n.content['process_id'] == 'resample_spatial':
                        for n_0 in n.dependencies: # Check if the (or one of the) resample_spatial process is related to this load_collection
                            parentID = n_0.id
                            continue
                        if parentID == node.id: # The found resample_spatial comes right after the current load_collection, let's apply the resampling to the query
                            if 'resolution' in n.arguments:
                                res = n.arguments['resolution']
                                if isinstance(res,float) or isinstance(res,int):
                                    resolutions = (res,res)
                                elif len(res) == 2:
                                    resolutions = (res[0],res[1])
                                else:
                                    print('error')

                            if 'projection' in n.arguments:
                                if n.arguments['projection'] is not None:
                                    projection = n.arguments['projection']
                                    if isinstance(projection,int):           # Check if it's an EPSG code and append 'epsg:' to it, without ODC returns an error
                                        ## TODO: make other projections available
                                        projection = 'epsg:' + str(projection)
                                    else:
                                        print('This type of reprojection is not yet implemented')
                                    outputCrs = projection

                            if 'method' in n.arguments:
                                resamplingMethod = n.arguments['method']

                odc = Odc(collections=collection,timeStart=timeStart,timeEnd=timeEnd,bands=bands,lowLat=lowLat,highLat=highLat,lowLon=lowLon,highLon=highLon,resolutions=resolutions,outputCrs=outputCrs,polygon=polygon,resamplingMethod=resamplingMethod)
                self.partialResults[node.id] = odc.data.to_array()
                self.crs = odc.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
                

                print(self.partialResults[node.id]) # The loaded data, stored in a dictionary with the id of the node that has generated it

            if processName == 'resample_spatial':
                source = node.arguments['data']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'resample_cube_temporal':
                target = node.arguments['target']['from_node']
                source = node.arguments['data']['from_node']
                def nearest(items, pivot):
                    return min(items, key=lambda x: abs(x - pivot))
                def resample_temporal(sourceCube,targetCube):
                    # Find in sourceCube the closest dates to tergetCube
                    newTime = []
                    for i,targetTime in enumerate(targetCube.time):
                        nearT = nearest(sourceCube.time.values,targetTime.values)
                        if i==0:
                            tmp = sourceCube.loc[dict(time=nearT)]
                        else:
                            tmp = xr.concat([tmp,sourceCube.loc[dict(time=nearT)]], dim='time')
                    tmp['time'] = targetCube.time
                    return tmp

                self.partialResults[node.id] = resample_temporal(self.partialResults[source],self.partialResults[target])


            if processName in ['multiply','divide','subtract','add','lt','lte','gt','gte','eq','neq']:
                if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['x']
                else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        source = node.parent_process.arguments['data']['from_node']
                    x = self.partialResults[source]
                if isinstance(node.arguments['y'],float) or isinstance(node.arguments['y'],int):
                    y = node.arguments['y']
                else:
                    if 'from_node' in node.arguments['y']:
                        source = node.arguments['y']['from_node']
                    elif 'from_parameter' in node.arguments['y']:
                        source = node.parent_process.arguments['data']['from_node']
                    y = self.partialResults[source]
                if processName == 'multiply':
                    self.partialResults[node.id] = x * y
                elif processName == 'divide':
                    self.partialResults[node.id] = x / y
                elif processName == 'subtract':
                    self.partialResults[node.id] = x - y
                elif processName == 'add':
                    self.partialResults[node.id] = x + y
                elif processName == 'lt':
                    self.partialResults[node.id] = x < y
                elif processName == 'lte':
                    self.partialResults[node.id] = x <= y
                elif processName == 'gt':
                    self.partialResults[node.id] = x > y
                elif processName == 'gte':
                    self.partialResults[node.id] = x >= y
                elif processName == 'eq':
                    self.partialResults[node.id] = x == y
                elif processName == 'neq':
                    self.partialResults[node.id] = x != y       

            if processName == 'not':
                if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['x']
                else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        source = node.parent_process.arguments['data']['from_node']
                    x = self.partialResults[source]
                self.partialResults[node.id] = np.logical_not(x)

            if processName == 'sum':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').sum().coarsen(y=yDim,boundary = 'pad').sum()
                else:
                    x = 0
                    for i,d in enumerate(node.arguments['data']):
                        if isinstance(d,float) or isinstance(d,int):         # We have to distinguish when the input data is a number or a datacube from a previous process
                            x = d
                        else:
                            if 'from_node' in d:
                                source = d['from_node']
                            elif 'from_parameter' in d:
                                source = node.parent_process.arguments['data']['from_node']
                            x = self.partialResults[source]
                        if i==0: self.partialResults[node.id] = x
                        else: self.partialResults[node.id] += x

            if processName == 'product':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').prod().coarsen(y=yDim,boundary = 'pad').prod()
                else:
                    x = 0
                    for i,d in enumerate(node.arguments['data']):
                        if isinstance(d,float) or isinstance(d,int):        # We have to distinguish when the input data is a number or a datacube from a previous process
                            x = d
                        else:
                            if 'from_node' in d:
                                source = d['from_node']
                            elif 'from_parameter' in d:
                                source = node.parent_process.arguments['data']['from_node']
                            x = self.partialResults[source]
                        if i==0: self.partialResults[node.id] = x
                        else: self.partialResults[node.id] *= x

            if processName == 'sqrt':
                x = node.arguments['x']
                if isinstance(x,float) or isinstance(x,int):        # We have to distinguish when the input data is a number or a datacube from a previous process
                    self.partialResults[node.id] = np.sqrt(x)
                else:
                    if 'from_node' in node.arguments['x']:
                        source = node.arguments['x']['from_node']
                    elif 'from_parameter' in node.arguments['x']:
                        source = node.parent_process.arguments['data']['from_node']
                    self.partialResults[node.id] = np.sqrt(self.partialResults[source])

            if processName == 'and':
                x = node.arguments['x']['from_node']
                y = node.arguments['y']['from_node']
                self.partialResults[node.id] = np.bitwise_and(self.partialResults[x],self.partialResults[y])

            if processName == 'or':
                x = node.arguments['x']['from_node']
                y = node.arguments['y']['from_node']
                self.partialResults[node.id] = np.bitwise_or(self.partialResults[x],self.partialResults[y])

            if processName == 'array_element':
                print(node.arguments)
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                print(source)
                noLabel = 1
                if 'label' in node.arguments:
                    if node.arguments['label'] is not None:
                        bandLabel = node.arguments['label']
                        noLabel = 0
                        self.partialResults[node.id] = self.partialResults[source].loc[dict(variable=bandLabel)]
                if 'index' in node.arguments and noLabel:
                    index = node.arguments['index']
                    self.partialResults[node.id] = self.partialResults[source][index]            


            if processName == 'normalized_difference':
                def normalized_difference(x,y):
                    return (x-y)/(x+y)
                xSource = (node.arguments['x']['from_node'])
                ySource = (node.arguments['y']['from_node'])
                self.partialResults[node.id] = normalized_difference(self.partialResults[xSource],self.partialResults[ySource])

            if processName == 'reduce_dimension':
                source = node.arguments['reducer']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'aggregate_spatial_window':
                source = node.arguments['reducer']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'max':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').max().coarsen(y=yDim,boundary = 'pad').max()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].max('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'min':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').min().coarsen(y=yDim,boundary = 'pad').min()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].min('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'mean':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').mean().coarsen(y=yDim,boundary = 'pad').mean()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].mean('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'median':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,boundary = 'pad').median().coarsen(y=yDim,boundary = 'pad').median()
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].median('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        print('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'power':
                dim = node.arguments['base']
                if isinstance(node.arguments['base'],float) or isinstance(node.arguments['base'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['base']
                else:
                    x = self.partialResults[node.arguments['base']['from_node']]
                self.partialResults[node.id] = x**node.arguments['p']

            if processName == 'absolute':
                source = node.arguments['x']['from_node']
                self.partialResults[node.id] = abs(self.partialResults[source])

            if processName == 'linear_scale_range':
                print(node.arguments)
                print(node.parent_process)
                parent = node.parent_process # I need to read the parent apply process
                if 'from_node' in parent.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    print('ERROR')
                inputMin = node.arguments['inputMin']
                inputMax = node.arguments['inputMax']
                outputMax = node.arguments['outputMax']
                outputMin = 0
                if 'outputMin' in node.arguments:
                    outputMin = node.arguments['outputMin']
                tmp = self.partialResults[source].clip(inputMin,inputMax)
                self.partialResults[node.id] = ((tmp - inputMin) / (inputMax - inputMin)) * (outputMax - outputMin) + outputMin

            if processName == 'filter_temporal':
                timeStart = node.arguments['extent'][0]
                timeEnd = node.arguments['extent'][1]
                if len(timeStart.split('T')) > 1:                # xarray slicing operation doesn't work with dates in the format 2017-05-01T00:00:00Z but only 2017-05-01
                    timeStart = timeStart.split('T')[0]
                if len(timeEnd.split('T')) > 1:
                    timeEnd = timeEnd.split('T')[0]
                source = node.arguments['data']['from_node']
                self.partialResults[node.id] = self.partialResults[source].loc[dict(time=slice(timeStart,timeEnd))]

            if processName == 'filter_bands':
                bandsToKeep = node.arguments['bands']
                source = node.arguments['data']['from_node']
                self.partialResults[node.id]  = self.partialResults[source].loc[dict(variable=bandsToKeep)]

            if processName == 'rename_labels':
                source = node.arguments['data']['from_node']
                try:
                    len(self.partialResults[source].coords['time'])
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                except:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                for i in range(len(node.arguments['target'])):
                    label_target = node.arguments['target'][i]
                    try:
                        node.arguments['source'][0]
                        label_source = node.arguments['source'][i]
                        tmp = tmp.assign({label_target:self.partialResults[source].loc[dict(variable=label_source)]})
                    except:
                        try:
                            self.partialResults[source].coords['variable']
                            tmp = tmp.assign({label_target:self.partialResults[source][i]})
                        except:
                            tmp = tmp.assign({label_target:self.partialResults[source]})
                self.partialResults[node.id] = tmp.to_array()

            if processName == 'add_dimension':
                print(node.content)

                source = node.arguments['data']['from_node']
                print(self.partialResults[source])
                try:
                    len(self.partialResults[source].coords['time'])
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                except:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                label_target = node.arguments['label']
                tmp = tmp.assign({label_target:self.partialResults[source]})
                self.partialResults[node.id] = tmp.to_array()
                print(self.partialResults[node.id])

            if processName == 'merge_cubes':
                cube1 = node.arguments['cube1']['from_node']
                cube2 = node.arguments['cube2']['from_node']
                ds1 = self.partialResults[cube1]
                ds2 = self.partialResults[cube2]
                self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')
                print(ds1,ds2)
                print(self.partialResults[node.id])
            if processName == 'if':
                acceptVal = None
                rejectVal = None
                valueVal  = None
                if isinstance(node.arguments['reject'],float) or isinstance(node.arguments['reject'],int):
                    rejectVal = node.arguments['reject']
                else:   
                    reject = node.arguments['reject']['from_node']
                    rejectVal = self.partialResults[reject]
                if isinstance(node.arguments['accept'],float) or isinstance(node.arguments['accept'],int):
                    acceptVal = node.arguments['accept']
                else:   
                    accept = node.arguments['accept']['from_node']
                    acceptVal = self.partialResults[accept]
                if isinstance(node.arguments['value'],float) or isinstance(node.arguments['value'],int):
                    valueVal = node.arguments['value']
                else:   
                    value = node.arguments['value']['from_node']
                    valueVal = self.partialResults[value]         

                tmpAccept = valueVal * acceptVal
                tmpReject = xr.ufuncs.logical_not(valueVal) * rejectVal
                self.partialResults[node.id] = tmpAccept + tmpReject     

            if processName == 'apply':
                source = node.arguments['process']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'mask':
                maskSource = node.arguments['mask']['from_node']
                dataSource = node.arguments['data']['from_node']
                # If the mask has a variable dimension, it will keep only the values of the input with the same variable name.
                # Solution is to take the min over the variable dim to drop that dimension. (Problems if there are more than 1 band/variable)
                if 'variable' in self.partialResults[maskSource].dims:
                    mask = self.partialResults[maskSource].min(dim='variable')
                else:
                    mask = self.partialResults[maskSource]
                self.partialResults[node.id] = self.partialResults[dataSource].where(xr.ufuncs.logical_not(mask))
                if 'replacement' in node.arguments:
                    burnValue  = node.arguments['replacement']
                    self.partialResults[node.id] = self.partialResults[node.id].fillna(burnValue)

            if processName == 'climatological_normal':
            #{'process_id': 'climatological_normal', 'arguments': {'data': {'from_node': 'masked_21'}, 'frequency': 'monthly', 'climatology_period': ['2015-08-01T00:00:00Z', '2018-08-31T23:59:59Z']}}
                source             = node.arguments['data']['from_node']
                frequency          = node.arguments['frequency']
                if 'climatology_period' in node.arguments:
                    climatology_period = node.arguments['climatology_period']
                    # Perform a filter_temporal and then compute the mean over a monthly period
                    timeStart = climatology_period[0]
                    timeEnd   = climatology_period[1]
                    if len(timeStart.split('T')) > 1:         # xarray slicing operation doesn't work with dates in the format 2017-05-01T00:00:00Z but only 2017-05-01
                        timeStart = timeStart.split('T')[0]
                    if len(timeEnd.split('T')) > 1:
                        timeEnd = timeEnd.split('T')[0]
                    tmp = self.partialResults[source].loc[dict(time=slice(timeStart,timeEnd))]
                else:
                    tmp = self.partialResults[source]
                if frequency=='monthly':
                    freq = 'time.month'
                else:
                    freq = None
                self.partialResults[node.id] = tmp.groupby(freq).mean("time")

            if processName == 'anomaly':
                source    = node.arguments['data']['from_node']
                normals   = node.arguments['normals']['from_node']
                frequency = node.arguments['frequency']
                if frequency=='monthly':
                    freq = 'time.month'
                else:
                    freq = None
                self.partialResults[node.id] = (self.partialResults[source].groupby(freq) - self.partialResults[normals]).drop('month')

            if processName == 'apply_kernel':
                def convolve(data, kernel, mode='constant', cval=0, fill_value=0):
                    dims = ('x','y')
                #   scipy.ndimage.convolve(input, weights, output=None, mode='reflect', cval=0.0, origin=0)
                    print(mode, cval)
                    convolved = lambda data: scipy.ndimage.convolve(data, kernel, mode=mode, cval=cval)

                    data_masked = data.fillna(fill_value)

                    return xr.apply_ufunc(convolved, data_masked,
                                          vectorize=True,
                                          dask='parallelized',
                                          input_core_dims = [dims],
                                          output_core_dims = [dims],
                                          output_dtypes=[data.dtype],
                                          dask_gufunc_kwargs={'allow_rechunk':True})

                print(node.arguments)
                kernel = np.array(node.arguments['kernel'])
                factor = node.arguments['factor']
                fill_value = 0
                source = node.arguments['data']['from_node']
                openeo_scipy_modes = {'replicate':'nearest','reflect':'reflect','reflect_pixel':'mirror','wrap':'wrap'}
                if 'replace_invalid' in node.arguments:
                    fill_value = node.arguments['replace_invalid']
                if 'border' in node.arguments:
                    if isinstance(node.arguments['border'],int) or isinstance(node.arguments['border'],float):
                        mode = 'constant'
                        cval = node.arguments['border']
                    else:
                        mode_openeo = node.arguments['border']
                        mode = openeo_scipy_modes[mode_openeo]
                        cval = 0
                self.partialResults[node.id] = convolve(self.partialResults[source],kernel,mode,cval,fill_value)
                if factor!=1:
                    self.partialResults[node.id] = self.partialResults[node.id] * factor

            if processName == 'geocode':
                from scipy.spatial import Delaunay
                from scipy.interpolate import LinearNDInterpolator

                print(node.arguments)
                source = node.arguments['data']['from_node']
                ## TODO: add check res and crs values, if None raise error
                spatialres = node.arguments['resolution']
                output_crs = "epsg:" + str(node.arguments['crs'])
                ## TODO: check if grid_lon and grid_lat are available, else raise error
                grid_lon = self.partialResults[source].loc[dict(variable='grid_lon')].values
                grid_lat = self.partialResults[source].loc[dict(variable='grid_lat')].values
                x_regular, y_regular, grid_x_irregular, grid_y_irregular = create_S2grid(grid_lon,grid_lat,output_crs,spatialres)
                grid_x_regular, grid_y_regular = np.meshgrid(x_regular,y_regular)
                grid_x_irregular = grid_x_irregular.astype(np.float32)
                grid_y_irregular = grid_y_irregular.astype(np.float32)
                x_regular = x_regular.astype(np.float32)
                y_regular = y_regular.astype(np.float32)
                grid_x_regular = grid_x_regular.astype(np.float32)
                grid_y_regular = grid_y_regular.astype(np.float32)
                grid_regular_flat = np.asarray([grid_x_regular.flatten(), grid_y_regular.flatten()]).T
                grid_irregular_flat = np.asarray([grid_x_irregular.flatten(), grid_y_irregular.flatten()]).T
                grid_x_irregular = None
                grid_y_irregular = None

                delaunay_obj = Delaunay(grid_irregular_flat)  # Compute the triangulation
                
                geocoded_cube = xr.Dataset(
                    coords={
                        "y": (["y"],y_regular),
                        "x": (["x"],x_regular)
                    },
                )

                def data_geocoding(data,grid_regular_flat):
                    flat_data = data.values.flatten()

                    def parallel_geocoding(subgrid):
                        interpolator  = LinearNDInterpolator(delaunay_obj, flat_data)
                        geocoded_data_slice = interpolator(subgrid)
                        return geocoded_data_slice

                    chunk_length = 20000000
                    subgrids = []
                    for i in range(int(len(grid_regular_flat)/chunk_length)+1):
                        if i<int(len(grid_regular_flat)/chunk_length):
                            grid_regular_flat_slice = grid_regular_flat[i*chunk_length:(i+1)*chunk_length]
                            subgrids.append(grid_regular_flat_slice)
                        else:
                            grid_regular_flat_slice = grid_regular_flat[i*chunk_length:]
                            subgrids.append(grid_regular_flat_slice)     

                    result = []
                    for s in subgrids:
                        result.append(delayed(parallel_geocoding)(s))

                    result = dask.compute(*result,scheduler='threads')
                    result_list = []
                    for r in result:
                        result_list += r.tolist()
                    result_arr = np.asarray(result_list)
                    return result_arr
                
                print("Geocoding started!")
                start = time()
                try: 
                    self.partialResults[source]['time']
                    for t in self.partialResults[source]['time']:
                        print(t.values)
                        geocoded_dataset = None
                        for var in self.partialResults[source]['variable']:
                            if (var.values!='grid_lon' and var.values!='grid_lat'):
                                data = self.partialResults[source].loc[dict(variable=var,time=t)]
                                geocoded_data = data_geocoding(data,grid_regular_flat).reshape(grid_x_regular.shape)
                                if geocoded_dataset is None:
                                    geocoded_dataset = geocoded_cube.assign_coords(time=t.values).expand_dims('time')
                                    geocoded_dataset[str(var.values)] = (("time","y", "x"),np.expand_dims(geocoded_data,axis=0))
                                else:
                                    geocoded_dataset[str(var.values)] = (("time","y", "x"),np.expand_dims(geocoded_data,axis=0))

                        geocoded_dataset.to_netcdf(self.tmpFolderPath+'/'+str(t.values)+'.nc')
                        geocoded_dataset = None
                    ## With a timeseries of geocoded data, I write every timestep, which can have multiple bands,
                    ## into a NetCDF and then I read the timeseries in chunks to avoid memory problems.
                    self.partialResults[node.id] = xr.open_mfdataset(self.tmpFolderPath + '/*.nc', combine="by_coords").to_array()
                except:
                    geocoded_dataset = None
                    for var in self.partialResults[source]['variable']:
                        if (var.values!='grid_lon' and var.values!='grid_lat'):
                            data = self.partialResults[source].loc[dict(variable=var)]
                            geocoded_data = data_geocoding(data,grid_regular_flat).reshape(grid_x_regular.shape)
                            if geocoded_dataset is None:
                                geocoded_cube[str(var.values)] = (("y", "x"),geocoded_data)
                                geocoded_dataset = geocoded_cube
                            else:
                                geocoded_dataset[str(var.values)] = (("y", "x"),geocoded_data)

                    self.partialResults[node.id] = geocoded_dataset
                
                print("Elapsed time: ", time() - start)

            if processName == 'save_result':
                outFormat = node.arguments['format']
                source = node.arguments['data']['from_node']
                print(self.partialResults[source])
                if outFormat=='PNG':
                    self.outFormat = '.png'
                    self.mimeType = 'image/png'
                    import cv2
                    self.partialResults[source] = self.partialResults[source].fillna(0)
                    size = None; red = None; green = None; blue = None; gray = None
                    if 'options' in node.arguments:
                        if 'size' in node.arguments['options']:
                            size = node.arguments['options']['size']
                        if 'red' in node.arguments['options']:
                            red = node.arguments['options']['red']
                        if 'green' in node.arguments['options']:
                            green = node.arguments['options']['green']
                        if 'blue' in node.arguments['options']:
                            blue = node.arguments['options']['blue']
                        if 'gray' in node.arguments['options']:
                            gray = node.arguments['options']['gray']
                        if red is not None and green is not None and blue is not None and gray is not None:
                            redBand   = self.partialResults[source].loc[dict(variable=red)].values
                            blueBand  = self.partialResults[source].loc[dict(variable=blue)].values
                            greenBand = self.partialResults[source].loc[dict(variable=green)].values
                            grayBand  = self.partialResults[source].loc[dict(variable=gray)].values
                            bgr = np.stack((blueBand,greenBand,redBand,grayBand),axis=2)
                        elif red is not None and green is not None and blue is not None:
                            redBand   = self.partialResults[source].loc[dict(variable=red)].values
                            blueBand  = self.partialResults[source].loc[dict(variable=blue)].values
                            greenBand = self.partialResults[source].loc[dict(variable=green)].values
                            bgr = np.stack((blueBand,greenBand,redBand),axis=2)
                        else:
                            bgr = self.partialResults[source].values
                            if bgr.shape[0] in [1,2,3,4]:
                                bgr = np.moveaxis(bgr,0,-1)
                    else:
                        bgr = self.partialResults[source].values
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
                    cv2.imwrite(self.tmpFolderPath + '/output.png',bgr)
                    return 0

                if outFormat=='GTiff' or outFormat=='GTIFF' or outFormat=='GEOTIFF':
                    self.outFormat = '.tif'
                    self.mimeType = 'image/tiff'
                    import rioxarray
                    if len(self.partialResults[source].dims) > 3:
                        if len(self.partialResults[source].time)>=1 and len(self.partialResults[source].variable)==1:
                            # We keep the time dimension as band in the GeoTiff, timeseries of a single band/variable
                            self.partialResults[node.id] = self.partialResults[source].squeeze('variable').to_dataset(name='result')
                        elif (len(self.partialResults[source].time==1) and len(self.partialResults[source].variable>=1)):
                            # We keep the time variable as band in the GeoTiff, multiple band/variables of the same timestamp
                            self.partialResults[node.id] = self.partialResults[source].squeeze('time')
                            geocoded_cube = xr.Dataset(
                                                        coords={
                                                            "y": (["y"],self.partialResults[node.id].y),
                                                            "x": (["x"],self.partialResults[node.id].x)
                                                        },
                                                    )
                            for var in self.partialResults[node.id]['variable']:
                                geocoded_cube[str(var.values)] = (("y", "x"),self.partialResults[node.id].loc[dict(variable=var.values)])
                            self.partialResults[node.id] = geocoded_cube
                        else:
                            raise Exception("[!] Not possible to write a 4-dimensional GeoTiff, use NetCDF instead.")
                    else:
                        self.partialResults[node.id] = self.partialResults[source] 
                    self.partialResults[node.id].attrs['crs'] = self.crs
                    self.partialResults[node.id].rio.to_raster(self.tmpFolderPath + "/output.tif")
                    return 0

                if outFormat in ['NETCDF','netcdf','NetCDF','netCDF','Netcdf']:
                    self.outFormat = '.nc'
                    self.mimeType = 'application/octet-stream'
                    print(self.partialResults[source])
    #                 self.partialResults[source].time.encoding['units'] = "seconds since 1970-01-01 00:00:00"
                    try:
                        self.partialResults[source].to_netcdf(self.tmpFolderPath + "/output.nc")
                    except:
                        pass
                    try:
                        self.partialResults[source].time.attrs.pop('units', None)
                        self.partialResults[source].to_netcdf(self.tmpFolderPath + "/output.nc")
                    except:
                        pass
                    return 0

                else:
                    raise Exception("[!] Output format not recognized/implemented!")



                # self.data.set_crs(self.crs)
                # rds4326 = self.data.rio.reproject("epsg:4326")
                # print(rds4326.rio.crs)

                return 0 # Save result is the end of the process graph
            
            self.listExecutedIds.append(node.id) # Store the processed nodes ids
            return 1 # Go on and process the next node
        
        except Exception as e:
            print(e)
            raise Exception("ODC Error in process: ",processName,'\n Full Python log:\n',str(e))
        