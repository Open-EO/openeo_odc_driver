# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   23/11/2020

import math
import json
import numpy as np
from datetime import datetime
from time import time
import xarray as xr
import rasterio.features
import rasterio
import dask
import os
from dask.distributed import Client
from openeo_pg_parser.translate import translate_process_graph

client = Client()

try:
    from odc_wrapper import Odc
except:
    pass
    
class OpenEO():
    def __init__(self,jsonProcessGraph,LOCAL_TEST=0):
        self.LOCAL_TEST = LOCAL_TEST
        self.jsonProcessGraph = jsonProcessGraph
        self.data = None
        self.listExecutedIds = []
        self.partialResults = {}
        self.crs = None
        self.bands = None
        self.graph = translate_process_graph(jsonProcessGraph).sort(by='result')
        self.outFormat = None
        self.i = 0
        for i in range(0,len(self.graph)+1):
            if not self.process_node(i):
                print('[*] Processing finished!')
                break

    def process_node(self,i):
        node = self.graph[i]
        processName = node.process_id
        print("Process id: {} Process name: {}".format(node.id,processName))
        
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
                            projection = n.arguments['projection']
                            if isinstance(projection,int):           # Check if it's an EPSG code and append 'epsg:' to it, without ODC returns an error
                                projection = 'epsg:' + str(projection)
                            else:
                                print('This type of reprojection is not yet implemented')
                            outputCrs = projection

                        if 'method' in n.arguments:
                            resamplingMethod = n.arguments['method']
            
            if self.LOCAL_TEST==0:
                odc = Odc(collections=collection,timeStart=timeStart,timeEnd=timeEnd,bands=bands,lowLat=lowLat,highLat=highLat,lowLon=lowLon,highLon=highLon,resolutions=resolutions,outputCrs=outputCrs,polygon=polygon,resamplingMethod=resamplingMethod)
                self.partialResults[node.id] = odc.data.to_array()
                self.crs = odc.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
            else:
                print('LOCAL TEST')
                if self.i==0:
                    datacubePath = './test_datacubes/' + os.path.split(self.jsonProcessGraph)[1].split('.json')[0] + '.nc'
                    print(datacubePath)
                    self.i += 1
                else:
                    datacubePath = './test_datacubes/' + os.path.split(self.jsonProcessGraph)[1].split('.json')[0] + str(self.i) + '.nc'

                print(datacubePath)
                ds = xr.open_dataset(datacubePath,chunks={})
                self.partialResults[node.id] = ds.to_array()
                #self.crs = ds.spatial_ref.crs_wkt.split('AUTHORITY')[-1].replace(']', '').replace('[', '').replace('"', '').replace(',', ':')     
                
            #write_dataset_to_netcdf(odc.data,'merge_cubes4.nc')
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
                if dim in ['t','temporal'] and 'time' in self.partialResults[source].dims:
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
                if dim in ['t','temporal'] and 'time' in self.partialResults[source].dims:
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
                if dim in ['t','temporal'] and 'time' in self.partialResults[source].dims:
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
                if dim in ['t','temporal'] and 'time' in self.partialResults[source].dims:
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
            print(self.partialResults[node.id])
            
        if processName == 'anomaly':
#{'process_id': 'anomaly', 'arguments': {'data': {'from_node': 'masked_21'}, 'frequency': 'monthly', 'normals': {'from_node': '11_11'}}}
            source             = node.arguments['data']['from_node']
            normals             = node.arguments['normals']['from_node']
            frequency          = node.arguments['frequency']
            if frequency=='monthly':
                freq = 'time.month'
            else:
                freq = None
            self.partialResults[node.id] = (self.partialResults[source].groupby(freq) - self.partialResults[normals]).drop('month')
            
            
        if processName == 'save_result':
            outFormat = node.arguments['format']
            source = node.arguments['data']['from_node']
            print(self.partialResults[source])
            if outFormat=='PNG':
                self.outFormat = '.png'
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
                cv2.imwrite('output.png',bgr)
                return 0

            if outFormat=='GTiff' or outFormat=='GTIFF':
                self.outFormat = '.tif'
                import rioxarray
                if len(self.partialResults[source].dims) > 3:
                    if len(self.partialResults[source].time)>=1 and len(self.partialResults[source].variable)==1:
                        # We keep the time dimension as band in the GeoTiff, timeseries of a single band/variable
                        self.partialResults[source] = self.partialResults[source].squeeze('variable')
                    elif (len(self.partialResults[source].time==1) and len(self.partialResults[source].variable>=1)):
                        # We keep the time variable as band in the GeoTiff, multiple band/variables of the same timestamp
                        self.partialResults[source] = self.partialResults[source].squeeze('time')
                    else:
                        raise "[!] Not possible to write a 4-dimensional GeoTiff, use NetCDF instead."
                self.partialResults[source] = self.partialResults[source].to_dataset(name='result').assign_attrs(crs=self.crs)
                self.partialResults[source].result.rio.to_raster("output.tif")
                return 0
            
            if outFormat=='NETCDF':
                self.outFormat = '.nc'
                self.partialResults[source].to_netcdf('output.nc')
                return 0
            
            else:
                raise "[!] Output format not recognized/implemented!"

            
           
            # self.data.set_crs(self.crs)
            # rds4326 = self.data.rio.reproject("epsg:4326")
            # print(rds4326.rio.crs)
            
            return 0 # Save result is the end of the process graph

        self.listExecutedIds.append(node.id) # Store the processed nodes ids
        return 1 # Go on and process the next node
        