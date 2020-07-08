# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   03/07/2020

import math
import json
import numpy as np
from datetime import datetime
from time import time
import xarray as xr
import rasterio.features
import rasterio
import dask

from openeo_pg_parser.translate import translate_process_graph

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
        self.graph = translate_process_graph(jsonProcessGraph).sort(by='dependency')
        if self.load_data():
            for i in range(1,len(self.graph)+1):
                if not self.process_node(i):
                    print('[*] Processing finished!')
                    break

    def load_data(self): # The graph starts always with a load_collection process
        for node in self.graph:
            processName = node.content['process_id']
            if processName == 'load_collection':
                print("Process id: {} Process name: {}".format(node.id,processName))
                defaultTimeStart = '1970-01-01'
                defaultTimeEnd = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
                timeStart   = defaultTimeStart
                timeEnd     = defaultTimeEnd
                collection  = None
                lowLat      = None
                highLat     = None
                lowLon      = None
                highLon     = None
                self.bands  = None # List of bands, we may need it in the array_element process
                resolutions = None # Touple
                output_crs  = None
                polygon     = None
                if 'bands' in node.content['arguments']:
                    self.bands = node.content['arguments']['bands']
                    if self.bands == []: self.bands = None
                        
                collection = node.content['arguments']['id']
                if collection is None:
                    raise Exception('[!] You must provide a collection which provides the data!')
                
                if node.content['arguments']['temporal_extent'] is not None:
                    timeStart  = node.content['arguments']['temporal_extent'][0]
                    timeEnd    = node.content['arguments']['temporal_extent'][1]
                    
                # If there is a bounding-box or a polygon we set the variables, otherwise we pass the defaults    
                if 'south' in node.content['arguments']['spatial_extent'] and \
                   'north' in node.content['arguments']['spatial_extent'] and \
                   'east'  in node.content['arguments']['spatial_extent'] and \
                   'west'  in node.content['arguments']['spatial_extent']:
                    lowLat     = node.content['arguments']['spatial_extent']['south']
                    highLat    = node.content['arguments']['spatial_extent']['north']
                    lowLon     = node.content['arguments']['spatial_extent']['east']
                    highLon    = node.content['arguments']['spatial_extent']['west']
                    
                elif 'coordinates' in node.content['arguments']['spatial_extent']:
                    # Pass coordinates to odc and process them there
                    polygon = node.content['arguments']['spatial_extent']['coordinates']
                if self.LOCAL_TEST==0:
                    odc = Odc(collections=collection,timeStart=timeStart,timeEnd=timeEnd,bands=self.bands,lowLat=lowLat,highLat=highLat,lowLon=lowLon,highLon=highLon,resolutions=resolutions,output_crs=output_crs,polygon=polygon)
                    self.partialResults[node.id] = odc.data.to_array()
                    self.crs = odc.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
                else:
                    print('LOCAL TEST')
                    datacubePath = './test_datacubes/' + self.jsonProcessGraph.split('/')[-1].split('.json')[0] + '.nc'
                    print(datacubePath)
                    ds = xr.open_dataset(datacubePath,chunks={})
                    self.partialResults[node.id] = ds.to_array()
                    self.crs = ds.spatial_ref.crs_wkt.split('AUTHORITY')[-1].replace(']', '').replace('[', '').replace('"', '').replace(',', ':')                    
                #write_dataset_to_netcdf(odc.data,'merge_cubes4.nc')

                print(self.partialResults[node.id]) # The loaded data, stored in a dictionary with the id of the node that has generated it
                self.listExecutedIds.append(node.id)
                return True
        
    def process_node(self,i):
        node = self.graph[i]
        processName = node.content['process_id']
        print("Process id: {} Process name: {}".format(node.id,processName))
                  
        if processName in ['multiply','divide','subtract','add','lt','lte','gt','gte']:
            if isinstance(node.content['arguments']['x'],float) or isinstance(node.content['arguments']['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                x = node.content['arguments']['x']
            else:
                x = self.partialResults[node.content['arguments']['x']['from_node']]
            if isinstance(node.content['arguments']['y'],float) or isinstance(node.content['arguments']['y'],int):
                y = node.content['arguments']['y']
            else:
                y = self.partialResults[node.content['arguments']['y']['from_node']]
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
        
        
        if processName == 'sum':
            x = 0
            for i,d in enumerate(node.content['arguments']['data']):
                if isinstance(d,float) or isinstance(d,int):         # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = d
                else:
                    x = self.partialResults[d['from_node']].astype(float)
                if i==0: self.partialResults[node.id] = x
                else: self.partialResults[node.id] += x
                
        if processName == 'product':
            x = 0
            for i,d in enumerate(node.content['arguments']['data']):
                if isinstance(d,float) or isinstance(d,int):        # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = d
                else:
                    x = self.partialResults[d['from_node']]
                if i==0: self.partialResults[node.id] = x
                else: self.partialResults[node.id] *= x
        
        if processName == 'array_element':
            source = node.content['arguments']['data']['from_node']
            if 'label' in node.content['arguments']:
                bandLabel = node.content['arguments']['label']
            elif 'index' in node.content['arguments']:
                bandLabel = self.bands[node.content['arguments']['index']]
            self.partialResults[node.id] = self.partialResults[source].loc[dict(variable=bandLabel)]
                    
        if processName == 'normalized_difference':
            def normalized_difference(x,y):
                return (x-y)/(x+y)
            xSource = (node.content['arguments']['x']['from_node'])
            ySource = (node.content['arguments']['y']['from_node'])
            self.partialResults[node.id] = normalized_difference(self.partialResults[xSource],self.partialResults[ySource])

        if processName == 'reduce_dimension':
            source = node.content['arguments']['reducer']['from_node']
            self.partialResults[node.id] = self.partialResults[source]

        if processName == 'max':
            parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the max
            dim = parent.content['arguments']['dimension']
            source = node.content['arguments']['data']['from_node']
            if dim in ['t','temporal']:
                self.partialResults[node.id] = self.partialResults[source].max('time')
            elif dim in ['bands']:
                self.partialResults[node.id] = self.partialResults[source].max('variable')
            elif dim in ['x']:
                self.partialResults[node.id] = self.partialResults[source].max('x')
            elif dim in ['y']:
                self.partialResults[node.id] = self.partialResults[source].max('y')
            else:
                print('[!] Max along dimension {} not yet implemented.'.format(dim))

        if processName == 'min':
            parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the min
            dim = parent.content['arguments']['dimension']
            source = node.content['arguments']['data']['from_node']
            if dim in ['t','temporal']:
                self.partialResults[node.id] = self.partialResults[source].min('time')
            elif dim in ['bands']:
                self.partialResults[node.id] = self.partialResults[source].min('variable')
            elif dim in ['x']:
                self.partialResults[node.id] = self.partialResults[source].min('x')
            elif dim in ['y']:
                self.partialResults[node.id] = self.partialResults[source].min('y')
            else:
                print('[!] Min along dimension {} not yet implemented.'.format(dim))
        
        if processName == 'mean':
            parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
            dim = parent.content['arguments']['dimension']
            source = node.content['arguments']['data']['from_node']
            if dim in ['t','temporal']:
                self.partialResults[node.id] = self.partialResults[source].mean('time')
            elif dim in ['bands']:
                self.partialResults[node.id] = self.partialResults[source].mean('variable')
            elif dim in ['x']:
                self.partialResults[node.id] = self.partialResults[source].mean('x')
            elif dim in ['y']:
                self.partialResults[node.id] = self.partialResults[source].mean('y')
            else:
                print('[!] Mean along dimension {} not yet implemented.'.format(dim))
        
        if processName == 'absolute':
            source = node.content['arguments']['x']['from_node']
            self.partialResults[node.id] = abs(self.partialResults[source])

        if processName == 'linear_scale_range':
            source = node.content['arguments']['x']['from_node']
            inputMin = node.content['arguments']['inputMin']
            inputMax = node.content['arguments']['inputMax']
            outputMax = node.content['arguments']['outputMax']
            outputMin = 0
            if 'outputMin' in node.content['arguments']:
                outputMin = node.content['arguments']['outputMin']
            tmp = self.partialResults[source].clip(inputMin,inputMax)
            self.partialResults[node.id] = (((self.partialResults[source] - inputMin) / (inputMax - inputMin)) *
                                                (outputMax - outputMin) + outputMin).clip(outputMin,outputMax)
            
        if processName == 'filter_temporal':
            timeStart = node.content['arguments']['extent'][0]
            timeEnd = node.content['arguments']['extent'][1]
            if len(timeStart.split('T')) > 1:                # xarray slicing operation doesn't work with dates in the format 2017-05-01T00:00:00Z but only 2017-05-01
                timeStart = timeStart.split('T')[0]
            if len(timeEnd.split('T')) > 1:
                timeEnd = timeEnd.split('T')[0]
            source = node.content['arguments']['data']['from_node']
            self.partialResults[node.id] = self.partialResults[source].loc[dict(time=slice(timeStart,timeEnd))]
                        
        if processName == 'filter_bands':
            bandsToKeep = node.content['arguments']['bands']
            source = node.content['arguments']['data']['from_node']
            self.partialResults[node.id]  = self.partialResults[source].loc[dict(variable=bandsToKeep)]

        if processName == 'rename_labels':
            source = node.content['arguments']['data']['from_node']
            label_target = (node.content['arguments']['target'])[0]
            label_source = self.partialResults[source].to_dataset().variable.item(0)
            tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
            tmp = tmp.assign({label_target:self.partialResults[source].loc[dict(variable=label_source)]})
            self.partialResults[node.id] = tmp.to_array()
        
        if processName == 'merge_cubes':
            cube1 = node.content['arguments']['cube1']['from_node']
            cube2 = node.content['arguments']['cube2']['from_node']
            self.partialResults[node.id] = xr.concat([self.partialResults[cube1],self.partialResults[cube2]],dim='variable')
            
        if processName == 'apply':
            source = node.content['arguments']['process']['from_node']
            self.partialResults[node.id] = self.partialResults[source]
                           
        if processName == 'mask':
            print(node.content)
            maskSource = node.content['arguments']['mask']['from_node']
            dataSource = node.content['arguments']['data']['from_node']
            self.partialResults[node.id] = self.partialResults[dataSource].where(self.partialResults[maskSource])
            if 'replacement' in node.content['arguments']:
                burnValue  = node.content['arguments']['replacement']
                self.partialResults[node.id] = self.partialResults[node.id].fillna(burnValue)
            
        if processName == 'save_result':
            outFormat = node.content['arguments']['format']
            source = node.content['arguments']['data']['from_node']
            if outFormat=='PNG':
                import cv2
                self.partialResults[source] = self.partialResults[source].fillna(0)
                size = None; red = None; green = None; blue = None; gray = None
                if 'options' in node.content['arguments']:
                    if 'size' in node.content['arguments']['options']:
                        size = node.content['arguments']['options']['size']
                    if 'red' in node.content['arguments']['options']:
                        red = node.content['arguments']['options']['red']
                    if 'green' in node.content['arguments']['options']:
                        green = node.content['arguments']['options']['green']
                    if 'blue' in node.content['arguments']['options']:
                        blue = node.content['arguments']['options']['blue']
                    if 'gray' in node.content['arguments']['options']:
                        gray = node.content['arguments']['options']['gray']
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
                cv2.imwrite('ouput.png',bgr)
                return 0

            if outFormat=='GTiff':
                import rioxarray
                self.partialResults[source] = self.partialResults[source].to_dataset(name='result').assign_attrs(crs=self.crs)
                self.partialResults[source].result.rio.to_raster("output.tif")
                return 0
            
            if outFormat=='netcdf':
                self.partialResults[source].to_netcdf('output.nc')
                return 0
                        
            return 0 # Save result is the end of the process graph

        self.listExecutedIds.append(node.id) # Store the processed nodes ids
        return 1 # Go on and process the next node
        