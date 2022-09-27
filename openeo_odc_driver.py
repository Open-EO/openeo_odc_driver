# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   23/03/2022

def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn

# Import necessary libraries
# System
import os
from os.path import exists
import sys
from time import time
from datetime import datetime
import json
import uuid
import pickle
import logging
import traceback
from glob import glob
# Math & Science
import math
import numpy as np
import scipy
from scipy.interpolate import griddata
from scipy.spatial import Delaunay
from scipy.interpolate import LinearNDInterpolator, NearestNDInterpolator
from scipy.optimize import curve_fit
import random
# Geography
from pyproj import Proj, transform, Transformer, CRS
import rasterio
import rasterio.features
import geopandas as gpd
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.multipoint import MultiPoint
from osgeo import gdal
# Machine Learning
from sklearn import tree, model_selection
from sklearn.metrics import accuracy_score
from sklearn import metrics
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
# Datacubes and databases
import datacube
import xarray as xr
import rioxarray
import pandas as pd
# Parallel Computing
import dask
from dask.distributed import Scheduler, Worker, Client, LocalCluster
from dask import delayed
from joblib import Parallel
from joblib import delayed as joblibDelayed
# openEO
from openeo_pg_parser.translate import translate_process_graph
# openeo_odc_driver
from openEO_error_messages import *
from odc_wrapper import Odc
from config import *
# SAR2Cube
try:
    from sar2cube_utils import *
except:
    pass
import asyncio
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("odc_openeo_engine.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class OpenEO():
    def __init__(self,jsonProcessGraph):
        self.jsonProcessGraph = jsonProcessGraph
        try:
            self.jobId = jsonProcessGraph['id']
        except:
            self.jobId = "None"
        self.data = None
        self.listExecutedIds = []
        self.partialResults = {}
        self.crs = None
        self.bands = None
        self.graph = translate_process_graph(jsonProcessGraph,process_defs=OPENEO_PROCESSES).sort(by='result')
        self.outFormat = None
        self.returnFile = True
        self.mimeType = None
        self.i = 0
        if self.jobId == "None":
            self.tmpFolderPath = TMP_FOLDER_PATH + str(uuid.uuid4())
        else:
            self.tmpFolderPath = TMP_FOLDER_PATH + self.jobId # If it is a batch job, there will be a field with it's id
        self.sar2cubeCollection = False
        self.fitCurveFunctionString = ""
        try:
            os.mkdir(self.tmpFolderPath)
        except:
            pass
        self.start = time()
        logging.info("[*] Init of dask cluster")
#         client = Client()
#         for i in range(0,len(self.graph)+1):
#             if not self.process_node(i):
#                 logging.info('[*] Processing finished!')
#                 logging.info('[*] Elaspsed time: ', time() - start)
#                 break
#####################################################
        # def startLocalCluster():
        try:
            # from dask_gateway import Gateway
            # gateway = Gateway("http://127.0.0.1:8000")
            # logging.info("[*] Creating a dask Gateway client")
            # cluster = gateway.new_cluster()
            # logging.info("[*] Getting the dask cluster client")
            # client = cluster.get_client()
            # logging.info("[*] Dask initialized correctly!")
            with LocalCluster(n_workers=8, threads_per_worker=1, processes=True,memory_limit='20GB') as cluster:
                with Client(cluster) as client:
                    dask.config.set({"distributed.comm.timeouts.tcp": "50s"})
                    for i in range(0,len(self.graph)+1):
                        if not self.process_node(i):
                            logging.info('[*] Processing finished!')
                            logging.info('[*] Total elapsed time: {}'.format(time() - self.start))
                            break
        except Exception as e:
            raise e
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(startLocalCluster())
        # loop.close()
        # startLocalCluster()
#####################################################


    def process_node(self,i):
        node = self.graph[i]
        processName = node.process_id
        logging.info("Process id: {} Process name: {}".format(node.id,processName))
        try:
            start_time_proc = time()
            if processName == 'load_collection':
                defaultTimeStart = '1970-01-01'
                defaultTimeEnd   = str(datetime.now()).split(' ')[0] # Today is the default date for timeEnd, to include all the dates if not specified
                timeStart        = defaultTimeStart
                timeEnd          = defaultTimeEnd
                collection       = None
                south            = None
                north            = None
                east             = None
                west             = None
                bands            = None # List of bands
                resolutions      = None # Tuple
                outputCrs        = None
                crs              = None
                resamplingMethod = None
                polygon          = None
                if 'bands' in node.arguments:
                    bands = node.arguments['bands']
                    if bands == []: bands = None

                collection = node.arguments['id'] # The datacube we have to load
                if collection is None:
                    raise Exception('[!] You must provide a collection name!')
                self.sar2cubeCollection = ('SAR2Cube' in collection) # Return True if it's a SAR2Cube collection
        
                if node.arguments['temporal_extent'] is not None:
                    timeStart  = node.arguments['temporal_extent'][0]
                    timeEnd    = node.arguments['temporal_extent'][1]

                # If there is a bounding-box or a polygon we set the variables, otherwise we pass the defaults
                if 'spatial_extent' in node.arguments and node.arguments['spatial_extent'] is not None:
                    if 'south' in node.arguments['spatial_extent'] and \
                       'north' in node.arguments['spatial_extent'] and \
                       'east'  in node.arguments['spatial_extent'] and \
                       'west'  in node.arguments['spatial_extent']:
                        south     = node.arguments['spatial_extent']['south'] #lowLat
                        north    = node.arguments['spatial_extent']['north'] #highLat
                        east     = node.arguments['spatial_extent']['east'] #lowLon
                        west    = node.arguments['spatial_extent']['west'] #highLon

                    elif 'coordinates' in node.arguments['spatial_extent']:
                        # Pass coordinates to odc and process them there
                        polygon = node.arguments['spatial_extent']['coordinates']
                        
                    if 'crs' in node.arguments['spatial_extent'] and node.arguments['spatial_extent']['crs'] is not None:
                        crs = node.arguments['spatial_extent']['crs']

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
                                    logging.info('error')

                            if 'projection' in n.arguments:
                                if n.arguments['projection'] is not None:
                                    projection = n.arguments['projection']
                                    if isinstance(projection,int):           # Check if it's an EPSG code and append 'epsg:' to it, without ODC returns an error
                                        ## TODO: make other projections available
                                        projection = 'epsg:' + str(projection)
                                    else:
                                        logging.info('This type of reprojection is not yet implemented')
                                    outputCrs = projection

                            if 'method' in n.arguments:
                                resamplingMethod = n.arguments['method']

                odc = Odc(collections=collection,timeStart=timeStart,timeEnd=timeEnd,bands=bands,south=south,north=north,west=west,east=east,resolutions=resolutions,outputCrs=outputCrs,polygon=polygon,resamplingMethod=resamplingMethod,crs=crs)
                if len(odc.data) == 0:
                    raise Exception("load_collection returned an empty dataset, please check the requested bands, spatial and temporal extent.")
                self.partialResults[node.id] = odc.data.to_array()
                self.crs = odc.data.crs             # We store the data CRS separately, because it's a metadata we may lose it in the processing
                logging.info(self.partialResults[node.id]) # The loaded data, stored in a dictionary with the id of the node that has generated it
                        
            if processName == 'resample_spatial':
                source = node.arguments['data']['from_node']
                self.partialResults[node.id] = self.partialResults[source]
            
            # The following code block handles the fit_curve and predict_curve processes, where we need to convert a process graph into a callable python function
            if node.parent_process is not None:
                if (node.parent_process.process_id=='fit_curve' or node.parent_process.process_id=='predict_curve'):
                    if processName in ['pi']:
                        if processName == 'pi':
                            self.partialResults[node.id] =  "np.pi"
                    if processName in ['array_element']:
                        self.partialResults[node.id] = 'a' + str(node.arguments['index'])
                    if processName in ['multiply','divide','subtract','add','sin','cos']:
                        x = None
                        y = None
                        source = None
                        if 'x' in node.arguments and node.arguments['x'] is not None:
                            if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                                x = str(node.arguments['x'])
                            else:
                                if 'from_node' in node.arguments['x']:
                                    source = node.arguments['x']['from_node']
                                elif 'from_parameter' in node.arguments['x']:
                                    x = 'x'
                                if source is not None:
                                    x = self.partialResults[source]
                        source = None        
                        if 'y' in node.arguments and node.arguments['y'] is not None:
                            if isinstance(node.arguments['y'],float) or isinstance(node.arguments['y'],int):
                                y = str(node.arguments['y'])
                            else:
                                if 'from_node' in node.arguments['y']:
                                    source = node.arguments['y']['from_node']
                                elif 'from_parameter' in node.arguments['y']:
                                    y = 'x'
                                if source is not None:
                                    y = self.partialResults[source]
                        if processName == 'multiply':
                            if x is None or y is None:
                                raise Exception(MultiplicandMissing)
                            else:
                                self.partialResults[node.id] = "(" + x  + "*" + y + ")"
                        elif processName == 'divide':
                            if y==0:
                                raise Exception(DivisionByZero)
                            else:
                                self.partialResults[node.id] = "(" + x + "/" + y + ")"
                        elif processName == 'subtract':
                            self.partialResults[node.id] = "(" + x + "-" + y + ")"
                        elif processName == 'add':
                            self.partialResults[node.id] = "(" + x + "+" + y + ")"
                        elif processName == 'sin':
                            self.partialResults[node.id] = "np.sin(" + x + ")"
                        elif processName == 'cos':
                            self.partialResults[node.id] = "np.cos(" + x + ")"
                    return 1

                
            if processName == 'run_udf':
                from openeo_r_udf import udf_lib
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')

                udf_dim = None
                if parent.dimension in ['t','temporal','DATE']:
                    udf_dim = 'time'
                elif parent.dimension in ['bands']:
                    udf_dim = 'variable'
                spatial_ref_dim = 'spatial_ref'

                self.partialResults[source] = self.partialResults[source].drop(spatial_ref_dim)
                print(self.partialResults[source])
                
                input_data = self.partialResults[source].compute()
                if 'time' in input_data:
                    input_data['time'] = input_data['time'].astype('str')

                self.partialResults[node.id] = udf_lib.execute_udf(process=parent.process_id,
                                                          udf_path=node.arguments['udf'],
                                                          data=input_data,
                                                          dimension=udf_dim,
                                                          context=node.arguments['context'])
                                
            if processName == 'resample_cube_spatial':
                target = node.arguments['target']['from_node']
                source = node.arguments['data']['from_node']
                method = node.arguments['method']
                if method is None:
                    method = 'nearest'
                if method == 'near':
                    method = 'nearest'
                try:
                    import odc.algo
                    self.partialResults[node.id] = odc.algo._warp.xr_reproject(self.partialResults[source].compute(),self.partialResults[target].geobox,resampling=method).compute()
                except Exception as e:
                    logging.info(e)
                    try:
                        self.partialResults[node.id] = self.partialResults[source].rio.reproject_match(self.partialResults[target],resampling=method)
                    except Exception as e:
                        raise Exception("ODC Error in process: ",processName,'\n Full Python log:\n',str(e))

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
                    if 'time' in tmp.dims:
                        tmp['time'] = targetCube.time
                    else:
                        tmp = tmp.expand_dims('time')
                        tmp['time'] = targetCube.time.values
                    return tmp
                self.partialResults[node.id] = resample_temporal(self.partialResults[source],self.partialResults[target])

            if processName in ['multiply','divide','subtract','add','lt','lte','gt','gte','eq','neq','log','ln']:
                x = None
                y = None
                source = None
                if 'x' in node.arguments and node.arguments['x'] is not None:
                    if isinstance(node.arguments['x'],float) or isinstance(node.arguments['x'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                        x = node.arguments['x']
                    else:
                        if 'from_node' in node.arguments['x']:
                            source = node.arguments['x']['from_node']
                        elif 'from_parameter' in node.arguments['x']:
                            if node.parent_process.process_id == 'merge_cubes':
                                source = node.parent_process.arguments['cube1']['from_node']
                            else:
                                source = node.parent_process.arguments['data']['from_node']
                        if source is not None:
                            x = self.partialResults[source]
                if 'y' in node.arguments and node.arguments['y'] is not None:
                    if isinstance(node.arguments['y'],float) or isinstance(node.arguments['y'],int):
                        y = node.arguments['y']
                    else:
                        if 'from_node' in node.arguments['y']:
                            source = node.arguments['y']['from_node']
                        elif 'from_parameter' in node.arguments['y']:
                            if node.parent_process.process_id == 'merge_cubes':
                                source = node.parent_process.arguments['cube2']['from_node']
                            else:
                                source = node.parent_process.arguments['data']['from_node']
                        if source is not None:
                            y = self.partialResults[source]

                if processName == 'multiply':
                    if x is None or y is None:
                        raise Exception(MultiplicandMissing)
                    else:
                        try:
                            result = (x * y)
                            if isinstance(result,int):
                                result = float(result)
                            if isinstance(result,float):
                                pass
                            else:
                                result = result.astype(np.float32)
                            self.partialResults[node.id] = result
                        except:
                            if hasattr(x,'chunks'):
                                x = x.compute()
                            if hasattr(y,'chunks'):
                                y = y.compute()
                            try:
                                self.partialResults[node.id] = (x * y).astype(np.float32).chunk()
                            except Exception as e:
                                raise e
                elif processName == 'divide':
                    if (isinstance(y,int) or isinstance(y,float)) and y==0:
                        raise Exception(DivisionByZero)
                    else:
                        try:
                            result = (x / y)
                            if isinstance(result,int):
                                result = float(result)
                            if isinstance(result,float):
                                pass
                            else:
                                result = result.astype(np.float32)
                            self.partialResults[node.id] = result
                        except:
                            if hasattr(x,'chunks'):
                                x = x.compute()
                            if hasattr(y,'chunks'):
                                y = y.compute()
                            try:
                                self.partialResults[node.id] = (x / y).astype(np.float32).chunk()
                            except Exception as e:
                                raise e
                elif processName == 'subtract':
                    try:
                        result = (x - y)
                        if isinstance(result,int):
                            result = float(result)
                        if isinstance(result,float):
                            pass
                        else:
                            result = result.astype(np.float32)
                        self.partialResults[node.id] = result
                    except:
                        if hasattr(x,'chunks'):
                            x = x.compute()
                        if hasattr(y,'chunks'):
                            y = y.compute()
                        try:
                            self.partialResults[node.id] = (x - y).astype(np.float32).chunk()
                        except Exception as e:
                            raise e
                elif processName == 'add':
                    try:
                        result = (x + y)
                        if isinstance(result,int):
                            result = float(result)
                        if isinstance(result,float):
                            pass
                        else:
                            result = result.astype(np.float32)
                        self.partialResults[node.id] = result
                    except:
                        if hasattr(x,'chunks'):
                            x = x.compute()
                        if hasattr(y,'chunks'):
                            y = y.compute()
                        try:
                            self.partialResults[node.id] = (x + y).astype(np.float32).chunk()
                        except Exception as e:
                            raise e
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
                elif processName == 'log':
                    base = float(node.arguments['base'])
                    self.partialResults[node.id] = np.log(x)/np.log(base) 
                elif processName == 'ln':
                    if isinstance(x,float) or isinstance(x,int):
                        self.partialResults[node.id] = np.ln(x)
                    else:    
                        self.partialResults[node.id] = np.ln(x)

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
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the sum
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).sum()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'sum' # Don't do anything, apply the sum later after aggregation
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
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the product
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    source = node.arguments['data']['from_node']
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).prod()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'product' # Don't do anything, apply the prod later after aggregation
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
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                noLabel = 1
                if 'label' in node.arguments:
                    if node.arguments['label'] is not None:
                        bandLabel = node.arguments['label']
                        noLabel = 0
                        self.partialResults[node.id] = self.partialResults[source].loc[dict(variable=bandLabel)].drop('variable')
                if 'index' in node.arguments and noLabel:
                    index = node.arguments['index']
                    self.partialResults[node.id] = self.partialResults[source][index]            
                    if 'variable' in self.partialResults[node.id].coords:
                        self.partialResults[node.id] = self.partialResults[node.id].drop('variable')

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

            if processName == 'aggregate_spatial':
                source = node.arguments['data']['from_node']
                geometries = node.arguments['geometries']
                for feature in geometries['features']:
                    if 'properties' not in feature:
                        feature['properties'] = {}
                    elif feature['properties'] is None:
                        feature['properties'] = {}
                gdf = gpd.GeoDataFrame.from_features(geometries['features'])

                ## Currently I suppose the input geometries are in EPSG:4326 and the collection is projected in UTM
                gdf = gdf.set_crs(4326)
                gdf_utm = gdf.to_crs(int(self.partialResults[source].spatial_ref))
                target_dimension = 'result'
                if 'target_dimension' in node.arguments:
                    target_dimension = node.arguments['target_dimension']
                ## First clip the data and keep only the data within the polygons
                crop = self.partialResults[source].rio.clip(gdf_utm.geometry, drop=True)
                reducer = self.partialResults[node.arguments['reducer']['from_node']]
                tmp = None
                ## Loop over the geometries in the FeatureCollection and apply the reducer
                for i in range(len(gdf_utm)):
                    if reducer == 'mean':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).mean(dim=['x','y'])
                    elif reducer == 'min':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).min(dim=['x','y'])
                    elif reducer == 'max':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).max(dim=['x','y'])
                    elif reducer == 'median':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).median(dim=['x','y'])
                    elif reducer == 'product':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).prod(dim=['x','y'])
                    elif reducer == 'sum':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).sum(dim=['x','y'])
                    elif reducer == 'sd':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).std(dim=['x','y'])
                    elif reducer == 'variance':
                        geom_crop = crop.rio.clip(gdf_utm.loc[[i]].geometry).std(dim=['x','y'])**2
                    geom_crop[target_dimension] = i
                    if tmp is not None:
                        tmp = xr.concat([tmp,geom_crop],dim=target_dimension)
                    else:
                        tmp = geom_crop
                self.partialResults[node.id] = tmp

            if processName == 'filter_spatial':
                source = node.arguments['data']['from_node']
                try:
                    gdf = gpd.GeoDataFrame.from_features(node.arguments['geometries']['features'])
                    ## Currently I suppose the input geometries are in EPSG:4326 and the collection is projected in UTM
                    gdf = gdf.set_crs(4326)
                except:
                    try:
                        coords = node.arguments['geometries']['coordinates']
                        print(coords)
                        polygon = Polygon([tuple(c) for c in coords[0]])
                        gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[polygon])
                    except Exception as e:
                        raise(e) 
                gdf_utm = gdf.to_crs(int(self.partialResults[source].spatial_ref))
                ## Clip the data and keep only the data within the polygons
                self.partialResults[node.id] = self.partialResults[source].rio.clip(gdf_utm.geometry, drop=True)


            if processName == 'max':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the max
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).max()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'max' # Don't do anything, apply the max later after aggregation
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
                        logging.info('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'min':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the min
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).min()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'min' # Don't do anything, apply the min later after aggregation
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
                        logging.info('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'mean':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the mean
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                self.partialResults[source] = self.partialResults[source].astype(np.float32)
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).mean()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'mean' # Don't do anything, apply the mean later after aggregation
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
                        logging.info('[!] Dimension {} not available in the current data.'.format(dim))

            if processName == 'median':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the median
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                self.partialResults[source] = self.partialResults[source].astype(np.float32)
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).median()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'median' # Don't do anything, apply the median later after aggregation
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
                        logging.info('[!] Dimension {} not available in the current data.'.format(dim))
                     
            if processName == 'sd':
                parent = node.parent_process # I need to read the parent reducer process to see along which dimension take the std
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.info('ERROR')
                if parent.content['process_id'] == 'aggregate_spatial_window':
                    xDim, yDim = parent.content['arguments']['size']
                    boundary = parent.content['arguments']['boundary']
                    self.partialResults[node.id] = self.partialResults[source].coarsen(x=xDim,y=yDim,boundary = boundary).std()
                elif parent.content['process_id'] in ['aggregate_temporal_period','aggregate_spatial']:
                    self.partialResults[node.id] = 'sd' # Don't do anything, apply the std later after aggregation
                else:
                    dim = parent.dimension
                    if dim in ['t','temporal','DATE'] and 'time' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('time')
                    elif dim in ['bands'] and 'variable' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('variable')
                    elif dim in ['x'] and 'x' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('x')
                    elif dim in ['y'] and 'y' in self.partialResults[source].dims:
                        self.partialResults[node.id] = self.partialResults[source].std('y')
                    else:
                        self.partialResults[node.id] = self.partialResults[source]
                        logging.info('[!] Dimension {} not available in the current data.'.format(dim))
            
            if processName == 'aggregate_temporal_period':
                source = node.arguments['data']['from_node']
                reducer = self.partialResults[node.arguments['reducer']['from_node']]
                #{'context': '', 'period': 'day', 'reducer': {'from_node': '1_2'}, 'data': {'from_node': '1_0'}, 'dimension': ''}
                period = node.arguments['period']
                #     hour: Hour of the day
                #     day: Day of the year
                #     week: Week of the year
                #     dekad: Ten day periods, counted per year with three periods per month (day 1 - 10, 11 - 20 and 21 - end of month). The third dekad of the month can range from 8 to 11 days. For example, the fourth dekad is Feb, 1 - Feb, 10 each year.
                #     month: Month of the year
                #     season: Three month periods of the calendar seasons (December - February, March - May, June - August, September - November).
                #     tropical-season: Six month periods of the tropical seasons (November - April, May - October).
                #     year: Proleptic years
                #     decade: Ten year periods (0-to-9 decade), from a year ending in a 0 to the next year ending in a 9.
                #     decade-ad: Ten year periods (1-to-0 decade) better aligned with the anno Domini (AD) calendar era, from a year ending in a 1 to the next year ending in a 0.
                periodsNotSupported = ['dekad','tropical-season','decade','decade-ad']
                #periodDict = {'hour':'time.hour','day':'time.dayofyear','week':'t.week','month':'time.month','season':'time.season','year':'year.time'}
                periodDict = {'hour':'1H','day':'1D','week':'1W','month':'1M','season':'QS','year':'1Y'}
                if period in periodsNotSupported:
                    raise Exception("The selected period " + period + " is not currently supported. Please use one of: " + list(periodDict.keys()))
                
                xarrayPeriod = str(periodDict[period])
                supportedReducers = ['sum','product','min','max','median','mean','sd']
                try:
                    if reducer == 'sum':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).sum()
                    elif reducer == 'product':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).prod()
                    elif reducer == 'min':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).min()
                    elif reducer == 'max':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).max()
                    elif reducer == 'median':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).median()
                    elif reducer == 'mean':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).mean()
                    elif reducer == 'sd':
                        self.partialResults[node.id] = self.partialResults[source].resample(time=xarrayPeriod).std()
                    else:
                        raise Exception("The selected reducer is not supported. Please use one of: " + supportedReducers)
                except Exception as e:
                    logging.info(e)
                    if reducer == 'sum':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).sum()
                    elif reducer == 'product':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).prod()
                    elif reducer == 'min':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).min()
                    elif reducer == 'max':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).max()
                    elif reducer == 'median':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).median()
                    elif reducer == 'mean':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).mean()
                    elif reducer == 'sd':
                        self.partialResults[node.id] = self.partialResults[source].resample(t=xarrayPeriod).std()
                    else:
                        raise Exception("The selected reducer is not supported. Please use one of: " + supportedReducers)  
                        
            if processName == 'power':
                dim = node.arguments['base']
                if isinstance(node.arguments['base'],float) or isinstance(node.arguments['base'],int): # We have to distinguish when the input data is a number or a datacube from a previous process
                    x = node.arguments['base']
                else:
                    x = self.partialResults[node.arguments['base']['from_node']]
                self.partialResults[node.id] = (x**node.arguments['p']).astype(np.float32)

            if processName == 'absolute':
                source = node.arguments['x']['from_node']
                self.partialResults[node.id] = abs(self.partialResults[source])

            if processName == 'linear_scale_range':
                parent = node.parent_process # I need to read the parent apply process
                if 'from_node' in parent.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    raise Exception("[!] The current process is missing the ['data']['from_node'] field, can't proceed.")
                inputMin = node.arguments['inputMin']
                inputMax = node.arguments['inputMax']
                outputMax = node.arguments['outputMax']
                outputMin = 0
                if 'outputMin' in node.arguments:
                    outputMin = node.arguments['outputMin']
                try:
                    tmp = self.partialResults[source].clip(min=inputMin,max=inputMax)
                except Exception as e:
                    logging.info(e)
                    try:
                        tmp = self.partialResults[source].compute()
                        tmp = tmp.clip(inputMin,inputMax)
                    except Exception as e:
                        raise e
                self.partialResults[node.id] = ((tmp - inputMin) / (inputMax - inputMin)) * (outputMax - outputMin) + outputMin

            if processName == 'clip':
                parent = node.parent_process # I need to read the parent apply process
                if 'from_node' in parent.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    raise Exception("[!] The current process is missing the ['data']['from_node'] field, can't proceed.")
                outputMax = node.arguments['max']
                outputMin = 0
                if 'min' in node.arguments:
                    outputMin = node.arguments['min']
                try:
                    tmp = self.partialResults[source].clip(outputMin,outputMax)
                    logging.info("[!] DASK CLIP FAILED, COMPUTING VALUES AND TRYING AGAIN")
                except:
                    try:
                        tmp = self.partialResults[source].compute()
                        tmp = tmp.fillna(0).clip(outputMin,outputMax).chunk(chunks={"time":-1,"x": 1000, "y":1000})
                    except Exception as e:
                        raise e
                self.partialResults[node.id] = tmp

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

            if processName == 'filter_bbox':
                source = node.arguments['data']['from_node']
                bbox_4326 = node.arguments['extent']
                if bbox_4326 is not None:
                    bbox_points_4326 = [[bbox_4326["south"],bbox_4326["west"]],
                       [bbox_4326["south"],bbox_4326["east"]],
                       [bbox_4326["north"],bbox_4326["east"]],
                       [bbox_4326["north"],bbox_4326["west"]]]
                    if "crs" in bbox_4326 and bbox_4326["crs"] is not None:
                        source_crs = bbox_4326["crs"]
                    else:
                        source_crs = 4326
                else:
                    raise Exception("[!] No spatial extent provided in filter_bbox !")
                    return
                try:
                    input_crs = str(self.partialResults[source].spatial_ref.values)
                except:
                    try:
                        input_crs = str(self.partialResults[source].spatial_ref)
                    except:
                        raise Exception("[!] Not possible to estimate the input data projection!")
                        return
                transformer = Transformer.from_crs("epsg:" + str(source_crs), "epsg:"+input_crs)
                
                x_t = []
                y_t = []
                for p in bbox_points_4326:     
                    x1,y1 = p
                    x2,y2 = transformer.transform(x1,y1)
                    x_t.append(x2)
                    y_t.append(y2)
                    
                x_t = np.array(x_t)
                y_t = np.array(y_t)
                x_min = x_t.min()
                x_max = x_t.max()
                y_min = y_t.min()
                y_max = y_t.max()
                
                self.partialResults[node.id] = self.partialResults[source].loc[dict(x=slice(x_min,x_max),y=slice(y_max,y_min))]
                if len(self.partialResults[node.id].y) == 0:
                    self.partialResults[node.id] = self.partialResults[source].loc[dict(x=slice(x_min,x_max),y=slice(y_min,y_max))]

            if processName == 'rename_labels':
                source    = node.arguments['data']['from_node']
                dimension = node.arguments['dimension']
                target_labels    = node.arguments['target']
                source_labels    = node.arguments['source']
                if dimension is None:
                    raise Exception("The mandatory field {} must be provided.".format('dimension'))
                if dimension not in ['t','time','temporal','DATE','bands','x','y','X','Y']:
                    raise Exception("The dimension field {} does not exist.".format(dimension))
                if dimension == 'bands':
                    if 'variable' not in self.partialResults[source].coords:
                        raise Exception(DimensionNotAvailable + "\nThe datacube does not have the dimension {} but only {}.".format(dimension,self.partialResults[source].coords.values))
                    # We need to create a new dataset, with time dimension if present.
                    if 'time' in self.partialResults[source].coords:
                        tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                    else:
                        tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                    for i in range(len(node.arguments['target'])):
                        label_target = node.arguments['target'][i]
                        if (len(node.arguments['source']))>0:
                            label_source = node.arguments['source'][i]
                            tmp = tmp.assign({label_target:self.partialResults[source].loc[dict(variable=label_source)]})
                        else:
                            if 'variable' in self.partialResults[source].coords:
                                tmp = tmp.assign({label_target:self.partialResults[source][i]})
                            else:
                                tmp = tmp.assign({label_target:self.partialResults[source]})
                    self.partialResults[node.id] = tmp.to_array()

                if dimension in ['t','temporal','time','DATE']:                    
                    if 'time' not in self.partialResults[source].coords:
                        raise Exception(DimensionNotAvailable + "\nThe datacube does not have the dimension {} but only {}.".format(dimension,self.partialResults[source].coords.values))
                    else:
                        if target_labels is not None and target_labels != []:
                            if len(target_labels) != len(self.partialResults[source]['time'].values):
                                raise Exception(LabelMismatch)
                            else:
                                self.partialResults[node.id] = self.partialResults[source]
                                self.partialResults[node.id]['time'] = np.asarray(target_labels)
                        else:
                            self.partialResults[node.id] = self.partialResults[source]
                            self.partialResults[node.id]['time'] = np.asarray(np.arange(0,len(self.partialResults[source]['time'].values)))
                        
            if processName == 'add_dimension':
                source = node.arguments['data']['from_node']
                try:
                    len(self.partialResults[source].coords['time'])
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x,'time':self.partialResults[source].time})
                except:
                    tmp = xr.Dataset(coords={'y':self.partialResults[source].y,'x':self.partialResults[source].x})
                label_target = node.arguments['label']
                tmp = tmp.assign({label_target:self.partialResults[source]})
                self.partialResults[node.id] = tmp.to_array()

            if processName == 'merge_cubes':
                # x,y,t + x,y (requires overlap resolver)
                # x,y,bands + x,y (requires overlap resolver)
                # x,y,t,bands + x,y (requires overlap resolver)
                # x,y,t,bands + x,y,bands falls into multiple categories. Depending on how the bands are structured. If they have the same bands, they need an overlap resolver. Bands that do only exist in one the cubes, get concatenated.
                
                ## dimensions check
                cube1 = node.arguments['cube1']['from_node']
                cube2 = node.arguments['cube2']['from_node']
                cube1_dims = self.partialResults[cube1].dims
                cube2_dims = self.partialResults[cube2].dims
                #dimensions are x, y, time, variable.
                if hasattr(cube1,'chunks') or hasattr(cube2,'chunks'):
                    # We need to re-chunk the data to avoid errors merging chunked and not chunked data
                    cube1 = cube1.chunk()
                    cube2 = cube2.chunk()
                logging.info("dimensions are x, y, time, variable.")
                if cube1_dims == cube2_dims:
                    # We need to check if they have bands
                    if 'variable' in cube1_dims and 'variable' in cube2_dims:
                        # We need to check if the bands are different or there are some common ones
                        logging.info("We need to check if the bands are different or there are some common ones")
                        cube1_bands = self.partialResults[cube1]['variable'].values
                        cube2_bands = self.partialResults[cube2]['variable'].values
                        try:
                            equal_bands = (cube1_bands == cube2_bands).all()
                        except:
                            try:
                                equal_bands = (cube1_bands == cube2_bands)
                            except:
                                equal_bands = False
                        if equal_bands and 'time' in cube1_dims: # ..or cube2_dims, they're equal here
                            # Simple case: same bands in both datacubes
                            logging.info("Simple case: same bands in both datacubes")
                            logging.info("We need to check if the timestep are different, if yes we can merge directly")
                            try:
                                all_timesteps_different = (self.partialResults[cube1].time.values != self.partialResults[cube2].time.values).all()
                            except:
                                try:
                                    all_timesteps_different = self.partialResults[cube1].time.values != self.partialResults[cube2].time.values
                                except Exception as e:
                                    raise e                                    
                            if all_timesteps_different:
                                self.partialResults[node.id] = xr.concat([self.partialResults[cube1],self.partialResults[cube2]],dim='time')
                            else:
                                #Overlap resolver required
                                logging.info("Overlap resolver required")
                                if 'overlap_resolver' in node.arguments:
                                    if 'from_node' in node.arguments['overlap_resolver']:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    else:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception(OverlapResolverMissing)
                        else:
                            #Check if at least one band is in common
                            logging.info("Check if at least one band is in common")
                            common_band = False
                            for v in cube1_bands:
                                if v in cube2_bands: common_band=True
                            if common_band:
                                #Complicate case where overlap_resolver has to be applied only on one or some bands
                                logging.info("Complicate case where overlap_resolver has to be applied only on one or some bands")
                                raise Exception("[!] Trying to merge two datacubes with one or more common bands, not supported yet!")
                            else:
                                #Simple case where all the bands are different and we can just concatenate the datacubes without overlap resolver
                                logging.info("Simple case where all the bands are different and we can just concatenate the datacubes without overlap resolver")
                                ds1 = self.partialResults[cube1]
                                ds2 = self.partialResults[cube2]
                                self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')
                    else:
                        # We don't have bands, dimensions are either x,y or x,y,t for both datacubes
                        logging.info("We don't have bands, dimensions are either x,y or x,y,t for both datacubes")
                        if 'time' in cube1_dims:
                            # TODO: check if the timesteps are the same, if yes use overlap resolver
                            cube1_time = self.partialResults[cube1].time.values
                            cube2_time = self.partialResults[cube2].time.values
                            if (cube1_time == cube2_time):
                                #Overlap resolver required
                                logging.info("Overlap resolver required")
                                if 'overlap_resolver' in node.arguments:
                                    try:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    except:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception(OverlapResolverMissing)
                            ## TODO: Case when only some timesteps are the same
                            ## TODO: Case when no timesteps are in common
                            else:
                                try:
                                    ds1 = self.partialResults[cube1]
                                    ds2 = self.partialResults[cube2]
                                    self.partialResults[node.id] = xr.concat([ds1,ds2],dim='time')
                                except Exception as e:
                                    raise e
                        if 't' in cube1_dims:
                            # TODO: check if the timesteps are the same, if yes use overlap resolver
                            cube1_time = self.partialResults[cube1].t.values
                            cube2_time = self.partialResults[cube2].t.values
                            if (cube1_time == cube2_time):
                                #Overlap resolver required
                                logging.info("Overlap resolver required")
                                if 'overlap_resolver' in node.arguments:
                                    try:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    except:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    raise Exception(OverlapResolverMissing)
                            ## TODO: Case when only some timesteps are the same
                            ## TODO: Case when no timesteps are in common
                            else:
                                try:
                                    ds1 = self.partialResults[cube1]
                                    ds2 = self.partialResults[cube2]
                                    self.partialResults[node.id] = xr.concat([ds1,ds2],dim='t')
                                except Exception as e:
                                    raise e
                        else:
                            # We have only x,y (or maybe only x or only y)
                            logging.info("We have only x,y (or maybe only x or only y)")
                            # Overlap resolver required
                            if 'overlap_resolver' in node.arguments and 'from_node' in node.arguments['overlap_resolver']:
                                source = node.arguments['overlap_resolver']['from_node']
                                self.partialResults[node.id] = self.partialResults[source]
                            else:
                                raise Exception(OverlapResolverMissing)
                else:
                    # CASE x,y,t + x,y (requires overlap resolver)
                    if 'time' in cube1_dims or 'time' in cube2_dims:
                            if 'x' and 'y' in cube1_dims and 'x' and 'y' in cube2_dims:
                                # We need to check if they have bands, if yes is still not possible
                                if 'variable' not in cube1_dims and 'variable' not in cube2_dims:
                                    #Overlap resolver required
                                    logging.info("Overlap resolver required")
                                    if 'overlap_resolver' in node.arguments and 'from_node' in node.arguments['overlap_resolver']:
                                        source = node.arguments['overlap_resolver']['from_node']
                                        self.partialResults[node.id] = self.partialResults[source]
                                    else:
                                        raise Exception(OverlapResolverMissing)
                                else:
                                    cube1 = node.arguments['cube1']['from_node']
                                    cube2 = node.arguments['cube2']['from_node']
                                    ds1 = self.partialResults[cube1]
                                    ds2 = self.partialResults[cube2]
                                    self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')
                    else:
                        cube1 = node.arguments['cube1']['from_node']
                        cube2 = node.arguments['cube2']['from_node']
                        ds1 = self.partialResults[cube1]
                        ds2 = self.partialResults[cube2]
                        self.partialResults[node.id] = xr.concat([ds1,ds2],dim='variable')

                    #raise Exception("[!] Trying to merge two datacubes with different dimensions, not supported yet!")
                
                
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
                tmpReject = np.logical_not(valueVal) * rejectVal
                self.partialResults[node.id] = tmpAccept + tmpReject     

            if processName == 'apply':
                source = node.arguments['process']['from_node']
                self.partialResults[node.id] = self.partialResults[source]

            if processName == 'apply_dimension':
                source = node.arguments['process']['from_node']
                self.partialResults[node.id] = self.partialResults[source]
                
            if processName == 'array_interpolate_linear':
                source = node.parent_process.arguments['data']['from_node']
                dimension = node.parent_process.arguments['dimension']
                if dimension in ['t','temporal','DATE','time']:
                    dimension = 'time'
                elif dimension in ['y', 'Y']:
                    dimension = 'y'
                elif dimension in ['x', 'X']:
                    dimension = 'x'
                else:
                    raise Exception("The selected dimension " + dimension + " is not supported.")
                self.partialResults[node.id] = self.partialResults[source].chunk({'time':-1}).interpolate_na(dim=dimension)

            if processName == 'mask':
                maskSource = node.arguments['mask']['from_node']
                dataSource = node.arguments['data']['from_node']
                # If the mask has a variable dimension, it will keep only the values of the input with the same variable name.
                # Solution is to take the min over the variable dim to drop that dimension. (Problems if there are more than 1 band/variable)
                if 'variable' in self.partialResults[maskSource].dims and len(self.partialResults[maskSource]['variable'])==1:
                    mask = self.partialResults[maskSource].min(dim='variable')
                else:
                    mask = self.partialResults[maskSource]
                self.partialResults[node.id] = self.partialResults[dataSource].where(np.logical_not(mask))
                if 'replacement' in node.arguments and node.arguments['replacement'] is not None:
                        burnValue  = node.arguments['replacement']
                        if isinstance(burnValue,int) or isinstance(burnValue,float):
                            self.partialResults[node.id] = self.partialResults[node.id].fillna(burnValue) #Replace the na with the burnValue
            
            if processName == 'climatological_normal':
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
                    convolved = lambda data: scipy.ndimage.convolve(data, kernel, mode=mode, cval=cval)

                    data_masked = data.fillna(fill_value)

                    return xr.apply_ufunc(convolved, data_masked,
                                          vectorize=True,
                                          dask='parallelized',
                                          input_core_dims = [dims],
                                          output_core_dims = [dims],
                                          output_dtypes=[data.dtype],
                                          dask_gufunc_kwargs={'allow_rechunk':True})

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
                def chunk_cube(data, size = 1024):
                    chunks = []
                    data_size = data.shape
                    num_chunks_x = int(np.ceil(data_size[1]/size))
                    num_chunks_y = int(np.ceil(data_size[0]/size))
                    for i in range(num_chunks_x):
                        x1 = i * size
                        x2 = min(x1 + size, data_size[1])
                        for j in range(num_chunks_y):
                            y1 = j * size
                            y2 = min(y1 + size, data_size[0])
                            chunk = data[y1:y2,x1:x2]
                            chunks.append(chunk)
                    return chunks
                
                def chunked_delaunay_interpolation(index,variable,output_path,chunk_x_regular,chunk_y_regular,grid_x_irregular,grid_y_irregular,numpy_data,resolution = 20,time = None):
                    offset = resolution*8 # Useful to get all the useful data from the irregular grid
                    grid_regular_flat = np.asarray([chunk_x_regular.flatten(), chunk_y_regular.flatten()]).T

                    chunk_x_min = np.min(chunk_x_regular) - offset
                    chunk_x_max = np.max(chunk_x_regular) + offset
                    chunk_y_min = np.min(chunk_y_regular) - offset
                    chunk_y_max = np.max(chunk_y_regular) + offset

                    chunk_mask = np.bitwise_and(np.bitwise_and(grid_x_irregular>chunk_x_min,grid_x_irregular<chunk_x_max),
                                                np.bitwise_and(grid_y_irregular>chunk_y_min,grid_y_irregular<chunk_y_max))

                    chunk_grid_x_irregular = grid_x_irregular[chunk_mask]
                    chunk_grid_y_irregular = grid_y_irregular[chunk_mask]
                    chunk_numpy_data       = numpy_data[chunk_mask]

                    grid_irregular_flat = np.asarray([chunk_grid_x_irregular, chunk_grid_y_irregular]).T

                    if grid_irregular_flat.shape[0] == 0:
                        # No data found for the provided area
                        empty_data = np.empty(chunk_x_regular.shape)
                        empty_data[:] = np.nan
                        return xr.DataArray(
                                            data = empty_data,
                                            dims=["y", "x"],
                                            coords=dict(
                                                y=(['y'], chunk_y_regular[:,0]),
                                                x=(['x'], chunk_x_regular[0,:])
                                            )
                                        )
                    write_delaunay = True
                    if os.path.exists(output_path + '/delaunay_{}.pc'.format(str(index))):
                        with open(output_path + '/delaunay_{}.pc'.format(str(index)), 'rb') as f:
                            delaunay_obj = pickle.load(f)
                        write_delaunay = False
                    else:        
                        delaunay_obj = Delaunay(grid_irregular_flat)
                    
                    grid_irregular_flat = None
                    func_nearest        = NearestNDInterpolator(delaunay_obj, chunk_numpy_data)
                    func_linear         = LinearNDInterpolator(delaunay_obj, np.zeros(chunk_numpy_data.shape))
                    # func_linear         = LinearNDInterpolator(delaunay_obj, chunk_numpy_data)
                    output_data         = func_nearest(grid_regular_flat)
                    output_data_linear  = func_linear(grid_regular_flat) # This mask can be reused
                    # output_data  = func_linear(grid_regular_flat) # This mask can be reused

                    if write_delaunay:
                        with open(output_path + '/delaunay_{}.pc'.format(str(index)), 'wb') as f:
                            pickle.dump(delaunay_obj,f)
                    da = xr.Dataset(
                        coords=dict(
                            y=(['y'], chunk_y_regular[:,0]),
                            x=(['x'], chunk_x_regular[0,:])
                        )
                    )
                    da[str(variable)] = (('y','x'),output_data.reshape(chunk_x_regular.shape))

                    da_linear = xr.Dataset(
                        coords=dict(
                            y=(['y'], chunk_y_regular[:,0]),
                            x=(['x'], chunk_x_regular[0,:])
                        )
                    )                    
                    da_linear[str(variable)] = (('y','x'),output_data_linear.reshape(chunk_x_regular.shape))
                    da = da.where(np.bitwise_not(np.isnan(da_linear)))
                    if time is not None:           
                        da = da.assign_coords(time=time).expand_dims('time')    
                    da.to_netcdf(output_path+'/'+str(time)+str(variable)+'{}.nc'.format(str(index)))
                    return

                source = node.arguments['data']['from_node']
                    
                if 'resolution' in node.arguments and node.arguments['resolution'] is not None:
                    spatialres = node.arguments['resolution']
                else:
                    raise Exception("[!] The geocode process is missing the required resolution field.")
                
                if spatialres not in [10,20,60]:
                    raise Exception("[!] The geocode process supports only 10m,20m,60m for resolution to align with the Sentinel-2 grid.")
                
                if 'crs' in node.arguments and node.arguments['crs'] is not None:
                    output_crs = "epsg:" + str(node.arguments['crs'])
                    self.crs = node.arguments['crs']
                else:
                    raise Exception("[!] The geocode process is missing the required crs field.")
                
                if 'grid_lon' in self.partialResults[source]['variable'] and 'grid_lat' in self.partialResults[source]['variable']:
                    pass
                else:
                    raise Exception("[!] The geocode process is missing the required grid_lon and grid_lat bands.")
                try: 
                    self.partialResults[source].loc[dict(variable='grid_lon')]
                    self.partialResults[source].loc[dict(variable='grid_lat')]
                    if len(self.partialResults[source].dims) >= 3:
                        if 'time' in self.partialResults[source].dims and len(self.partialResults[source].loc[dict(variable='grid_lon')].dims)>2:
                            grid_lon = self.partialResults[source].loc[dict(variable='grid_lon',time=self.partialResults[source].time[0])].values
                            grid_lat = self.partialResults[source].loc[dict(variable='grid_lat',time=self.partialResults[source].time[0])].values
                        else:
                            grid_lon = self.partialResults[source].loc[dict(variable='grid_lon')].values
                            grid_lat = self.partialResults[source].loc[dict(variable='grid_lat')].values
                except Exception as e:
                    traceback.print_exception(*sys.exc_info())
                    raise(e)
                                                          
                x_regular, y_regular, grid_x_irregular, grid_y_irregular = create_S2grid(grid_lon,grid_lat,output_crs,spatialres)
                grid_x_regular, grid_y_regular = np.meshgrid(x_regular,y_regular)

                grid_x_regular = grid_x_regular.astype(np.float32)
                grid_y_regular = grid_y_regular.astype(np.float32)
                
                chunks_x_regular = chunk_cube(grid_x_regular,size=512)
                chunks_y_regular = chunk_cube(grid_y_regular,size=512)
                grid_x_regular_shape = grid_x_regular.shape
                grid_x_regular = None
                grid_y_regular = None
                # grid_x_irregular = None
                # grid_y_irregular = None
                
                logging.info("Geocoding started!")
                if 'time' in self.partialResults[source].dims:
                    for t in self.partialResults[source]['time']:
                        geocoded_dataset = None
                        data_t = self.partialResults[source].loc[dict(time=t)]
                        for var in self.partialResults[source]['variable']:
                            if (var.values!='grid_lon' and var.values!='grid_lat'):
                                logging.info("Geocoding band {} for date {}".format(var.values,t.values))
                                numpy_data = data_t.loc[dict(variable=var)].values
                                print(numpy_data.shape)
                                Parallel(n_jobs=12, verbose=51)(
                                    joblibDelayed(chunked_delaunay_interpolation)(index,
                                                                                  var.values,
                                                                                  self.tmpFolderPath,
                                                                                  chunks_x_regular[index],
                                                                                  chunks_y_regular[index],
                                                                                  grid_x_irregular,
                                                                                  grid_y_irregular,
                                                                                  numpy_data,
                                                                                  spatialres,
                                                                                  t.values)for index in range(len(chunks_x_regular)))
                else:
                    geocoded_dataset = None
                    for var in self.partialResults[source]['variable']:
                        if (var.values!='grid_lon' and var.values!='grid_lat'):
                            logging.info("Geocoding band {}".format(var.values))
                            numpy_data = self.partialResults[source].loc[dict(variable=var)].values
                            Parallel(n_jobs=12, verbose=51)(
                                joblibDelayed(chunked_delaunay_interpolation)(index,
                                                                              var.values,
                                                                              self.tmpFolderPath,
                                                                              chunks_x_regular[index],
                                                                              chunks_y_regular[index],
                                                                              grid_x_irregular,
                                                                              grid_y_irregular,
                                                                              numpy_data,
                                                                              spatialres)for index in range(len(chunks_x_regular)))
                self.partialResults[node.id] = xr.open_mfdataset(self.tmpFolderPath + '/*.nc', combine="by_coords").to_array()
                self.partialResults[node.id] = self.partialResults[node.id].sortby(self.partialResults[node.id].y)

                            
                tmp_files_to_remove = glob(self.tmpFolderPath + '/*.pc')
                Parallel(n_jobs=8)(joblibDelayed(os.remove)(file_to_remove) for file_to_remove in tmp_files_to_remove)
            
            if processName == 'radar_mask':
                parent = node.parent_process
                if 'from_node' in node.arguments['data']:
                    source = node.arguments['data']['from_node']
                elif 'from_parameter' in node.arguments['data']:
                    source = node.parent_process.arguments['data']['from_node']
                else:
                    logging.error('ERROR')
                
                if 'foreshortening_th' in node.arguments and node.arguments['foreshortening_th'] is not None:
                    foreshortening_th = float(node.arguments['foreshortening_th'])
                else:
                    raise Exception("[!] You need to provide the foreshortening_th parameter to compute the radar masks!")
                if 'layover_th' in node.arguments and node.arguments['layover_th'] is not None:
                    layover_th = float(node.arguments['layover_th'])
                else:
                    raise Exception("[!] You need to provide the layover_th parameter to compute the radar masks!")
                if 'orbit_direction' in node.arguments and node.arguments['orbit_direction'] is not None:
                    orbit_direction = node.arguments['orbit_direction']
                else:
                    raise Exception("[!] You need to provide the orbit_direction parameter to compute the radar masks!")
                    
                src = self.partialResults[source]
                samples_dem = len(src.loc[dict(variable='DEM')].x)
                lines_dem   = len(src.loc[dict(variable='DEM')].y)
                dx = src.loc[dict(variable='DEM')].x[1].values - src.loc[dict(variable='DEM')].x[0].values  # Change based on geocoding output
                dy = src.loc[dict(variable='DEM')].y[1].values - src.loc[dict(variable='DEM')].y[0].values
                demdata = src.loc[dict(variable='DEM')].values
                # heading for sentinel:
                # ASC = -12.5°
                # DSC = +12.5°
                # Convert to radians before the usage
                heading = -12.5*np.pi/180 #ASC
                if orbit_direction == 'DSC':
                    heading = 12.5*np.pi/180
                dx_p=dx*np.tan(heading)
                dy_p=dy*np.tan(heading)
                daz = 2*np.sqrt(dy_p**2+dy**2)
                drg = 2*np.sqrt(dx_p**2+dx**2)
                h_az_0 = demdata[0:lines_dem-3,0:samples_dem-3] + (demdata[0:lines_dem-3,2:samples_dem-1] - demdata[0:lines_dem-3,0:samples_dem-3])/(2*dx)*(dx+dx_p)
                h_az_2 = demdata[2:lines_dem-1,0:samples_dem-3] + (demdata[2:lines_dem-1,2:samples_dem-1] - demdata[2:lines_dem-1,0:samples_dem-3])/(2*dx)*(dx-dx_p)
                inc_h_az = -(h_az_2-h_az_0)
                h_rg_0=demdata[0:lines_dem-3,0:samples_dem-3] + (demdata[2:lines_dem-1,0:samples_dem-3] - demdata[0:lines_dem-3,0:samples_dem-3])/(2*dy)*(dy-dy_p)
                h_rg_2=demdata[0:lines_dem-3,2:samples_dem-1] + (demdata[2:lines_dem-1,2:samples_dem-1] - demdata[0:lines_dem-3,2:samples_dem-1])/(2*dy)*(dy+dy_p)
                inc_h_rg=h_rg_2-h_rg_0
                rg_sign = 0
                az_sign = 0
                if heading >= 0:
                    az_sign=-1
                    rg_sign=-1 
                else:
                    az_sign=1
                    rg_sign=1
                res_out_f = np.zeros((demdata.shape))
                res_out_o = np.zeros((demdata.shape))
                res_out_f[1:lines_dem-2,1:samples_dem-2] = np.arctan(inc_h_rg/drg)*rg_sign # range
                res_out_o[1:lines_dem-2,1:samples_dem-2] = np.arctan(inc_h_az/daz)*az_sign
                res_out_f_deg = res_out_f*180/np.pi
                res_out_o_deg = res_out_o*180/np.pi
                mean_incAngle = np.nanmean(src.loc[dict(variable='LIA')].values)
                #foreshortening
                foreshortening     = np.bitwise_and(res_out_f_deg > 0,res_out_f_deg < mean_incAngle)*res_out_f_deg / mean_incAngle
                foreshortening_mask = np.zeros((demdata.shape))
                foreshortening_mask = (foreshortening > foreshortening_th).astype(np.float32)
                #layover
                layover        = np.bitwise_and(res_out_f_deg > 0,res_out_f_deg > mean_incAngle)*res_out_f_deg / mean_incAngle
                layover_mask   = (layover > layover_th).astype(np.float32)
                #shadowing
                shadow_mask      = np.bitwise_and(res_out_f_deg  < 0,np.abs(res_out_f_deg) > (90-mean_incAngle)).astype(np.float32)
                
                output = xr.ones_like(src.loc[dict(variable='DEM')]).drop('variable')
                layover_mask_da = (output * layover_mask).clip(0,1)
                layover_mask_da['variable'] = 1
                foreshortening_mask_da = (output * foreshortening_mask).clip(0,1)
                foreshortening_mask_da['variable'] = 2
                shadow_mask_da = (output * shadow_mask).clip(0,1)
                shadow_mask_da['variable'] = 3
                
                self.partialResults[node.id] = xr.concat([layover_mask_da,foreshortening_mask_da,shadow_mask_da],dim='variable')
                


            if processName == 'coherence':
                source = node.arguments['data']['from_node']
                timedelta = 6
                if 'timedelta' in node.arguments:
                    timedelta =  int(node.arguments['timedelta'])
                else:
                    pass
                    
                # We put the timesteps of the datacube into an array
                timesteps = self.partialResults[source]['time'].values
                days_pairs = []
                # We loop through the timesteps and check where we have 6-12-24 days pairs of dates

                tmp_dataset_timeseries = None
                for i,t in enumerate(timesteps[:-1]):
                    for t2 in timesteps[i+1:]:
                        if(np.timedelta64(t2 - t, 'D')) == np.timedelta64(timedelta,'D'):
                            days_pairs.append([t,t2])
                
                src = self.partialResults[source]
                for i,pair in enumerate(days_pairs):
                    logging.info(pair)
                    VV_q_coh = (src.loc[dict(variable='i_VV',time=pair[0])]*src.loc[dict(variable='i_VV',time=pair[1])]+src.loc[dict(variable='q_VV',time=pair[0])]*src.loc[dict(variable='q_VV',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VV',time=pair[0])]**2+src.loc[dict(variable='q_VV',time=pair[0])]**2)*(src.loc[dict(variable='i_VV',time=pair[1])]**2+src.loc[dict(variable='q_VV',time=pair[1])]**2))
                    VV_i_coh = (src.loc[dict(variable='i_VV',time=pair[1])]*src.loc[dict(variable='q_VV',time=pair[0])]-src.loc[dict(variable='i_VV',time=pair[0])]*src.loc[dict(variable='q_VV',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VV',time=pair[0])]**2+src.loc[dict(variable='q_VV',time=pair[0])]**2)*(src.loc[dict(variable='i_VV',time=pair[1])]**2+src.loc[dict(variable='q_VV',time=pair[1])]**2))
                    
                    VH_q_coh = (src.loc[dict(variable='i_VH',time=pair[0])]*src.loc[dict(variable='i_VH',time=pair[1])]+src.loc[dict(variable='q_VH',time=pair[0])]*src.loc[dict(variable='q_VH',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VH',time=pair[0])]**2+src.loc[dict(variable='q_VH',time=pair[0])]**2)*(src.loc[dict(variable='i_VH',time=pair[1])]**2+src.loc[dict(variable='q_VH',time=pair[1])]**2))
                    VH_i_coh = (src.loc[dict(variable='i_VH',time=pair[1])]*src.loc[dict(variable='q_VH',time=pair[0])]-src.loc[dict(variable='i_VH',time=pair[0])]*src.loc[dict(variable='q_VH',time=pair[1])])/                    np.sqrt((src.loc[dict(variable='i_VH',time=pair[0])]**2+src.loc[dict(variable='q_VH',time=pair[0])]**2)*(src.loc[dict(variable='i_VH',time=pair[1])]**2+src.loc[dict(variable='q_VH',time=pair[1])]**2))
                                                 
                    tmp_dataset = xr.Dataset(
                        coords={
                            "y": (["y"],self.partialResults[source].y.values),
                            "x": (["x"],self.partialResults[source].x.values)
                        },
                    )
                    tmp_dataset = tmp_dataset.assign_coords(time=pair[0]).expand_dims('time')
                    tmp_dataset['i_VV'] = (("time","y", "x"),VV_i_coh.expand_dims('time').values)
                    tmp_dataset['q_VV'] = (("time","y", "x"),VV_q_coh.expand_dims('time').values)
                    tmp_dataset['i_VH'] = (("time","y", "x"),VH_i_coh.expand_dims('time').values)
                    tmp_dataset['q_VH'] = (("time","y", "x"),VH_q_coh.expand_dims('time').values)
                    tmp_dataset.to_netcdf(self.tmpFolderPath+'/coh_'+str(t)+'.nc')
                    tmp_dataset = None
                    
                self.partialResults[node.id] = xr.open_mfdataset(self.tmpFolderPath + '/coh_*.nc', combine="by_coords").to_array()
                
            if processName == 'fit_curve':
                ## The fitting function as been converted in a dedicated if statement into a string
                fitFunction = self.partialResults[node.arguments['function']['from_node']]
                ## The data can't contain NaN values, they are replaced with zeros
#                 data = self.partialResults[node.arguments['data']['from_node']].compute().fillna(0)
                data = self.partialResults[node.arguments['data']['from_node']].fillna(0)
                data.name = None
                data_dataset = data.to_dataset(dim='variable').chunk({'time':-1,'x':512,'y':512})
                # data_dataset = data_dataset.rename({'t':'time'})
                baseParameters = node.arguments['parameters'] ## TODO: take care of them, currently ignored

                ## Preparation of fitting functions:
                def build_fitting_functions():
                    baseFun = "def fitting_function(x"
                    parametersStr = ""
                    for i in range(len(baseParameters)):
                        parametersStr += ",a"+str(i)
                    baseFun += (parametersStr + "):")
                    baseFun += ('''
    return '''+ fitFunction)
                    return baseFun
                ## Generate python fitting function as string 
                fitFun = build_fitting_functions()
                logging.info(fitFun)
                exec(fitFun,globals())
                def fit_curve(x,y):
                    index = np.nonzero(y) # We don't consider zero values (masked) for fitting.
                    x = x[index]
                    y = y[index]
                    if len(x)<12:
                        return np.array([0,0,0])
                    popt, pcov = curve_fit(fitting_function, x, y)
                    return popt
                
                
                dates = data_dataset.time.values
                unixSeconds = [ ((x - np.datetime64('1970-01-01')) / np.timedelta64(1, 's')) for x in dates]
                data_dataset['time'] = unixSeconds
                popts3d = xr.apply_ufunc(fit_curve,data_dataset.time,data_dataset,
                           vectorize=True,
                           input_core_dims=[['time'],['time']], #Dimension along we fit the curve function
                           output_core_dims=[['params']],
                           dask="parallelized",
                           output_dtypes=[np.float32],
                           dask_gufunc_kwargs={'allow_rechunk':True,'output_sizes':{'params':len(baseParameters)}}
                            )
                data_dataset['time'] = dates
                     
                self.partialResults[node.id] = popts3d.compute()
                logging.info("Elapsed time: ",time() - start)
                   
            if processName == 'predict_curve':
                start = time()
                fitFunction = self.partialResults[node.arguments['function']['from_node']]
                data = self.partialResults[node.arguments['data']['from_node']]
                dates = data.time.values
                unixSeconds = [ ((x - np.datetime64('1970-01-01')) / np.timedelta64(1, 's')) for x in dates]
                data['time'] = unixSeconds
                baseParameters = self.partialResults[node.arguments['parameters']['from_node']]
                if isinstance(baseParameters,xr.Dataset):
                    baseParameters = baseParameters.to_array()
                def build_fitting_functions():
                    baseFun = "def predicting_function(x"
                    parametersStr = ""
                    for i in range(len(baseParameters.params)):
                        parametersStr += ",a"+str(i) + "=0"
                    baseFun += (parametersStr + "):")
                    baseFun += ('''
    return '''+ fitFunction)
                    return baseFun
                fitFun = build_fitting_functions()
                logging.info(fitFun)
                exec(fitFun,globals())
                if 'variable' in data.dims:
                    predictedData = xr.Dataset(coords={'time':dates,'y':data.y,'x':data.x})
                    for var in data['variable'].values:
                        input_params = {}
                        for i in range(len(baseParameters.params)):
                            try:
                                band_parameter = baseParameters.loc[dict(variable=var)].drop('variable')[:,:,i]
                            except:
                                band_parameter = baseParameters.loc[dict(variable=var)][:,:,i]
                            input_params['a'+str(i)] = band_parameter
                        tmp_var = predicting_function(data.time,**input_params).astype(np.float32)
                        tmp_var['time'] = dates
                        predictedData[var] = tmp_var
                else:
                    predictedData = predicting_function(data.time,baseParameters[0,:,:].drop('variable'),baseParameters[1,:,:].drop('variable'),baseParameters[2,:,:].drop('variable'))
                
                data['time'] = dates
                self.partialResults[node.id] = predictedData.to_array().transpose('variable','time','y','x')
                
            if processName == 'load_result':
                from os.path import exists
                logging.info(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc')
                if exists(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc'):
                    try:
                        # If the data is has a single band we load it directly as xarray.DataArray, otherwise as Dataset and convert to DataArray
                        self.partialResults[node.id] = xr.open_dataarray(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc',chunks={})
                    except:
                        logging.info("Except")
                        self.partialResults[node.id] = xr.open_dataset(TMP_FOLDER_PATH + node.arguments['id'] + '/output.nc',chunks={}).to_array()
                else:
                    raise Exception("[!] Result of job " + node.arguments['id'] + " not found! It must be a netCDF file.")
                logging.info(self.partialResults[node.id])
                
            if processName == 'save_result':
                outFormat = node.arguments['format']
                source = node.arguments['data']['from_node']
                logging.info(self.partialResults[source])

                if outFormat.lower() == 'png':
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
                    bgr = bgr.astype(np.uint8)
                    if(self.sar2cubeCollection): bgr=np.flipud(bgr)
                    cv2.imwrite(str(self.tmpFolderPath) + '/result.png',bgr)
                    return 0

                if outFormat.lower() in ['gtiff','geotiff','tif','tiff']:
                    self.outFormat = '.tiff'
                    self.mimeType = 'image/tiff'
                    import rioxarray
                    
                    if self.partialResults[source].dtype == 'bool':
                        self.partialResults[source] = self.partialResults[source].astype(np.uint8)
                    
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
                    if 'variable' in self.partialResults[node.id].dims:
                        self.partialResults[node.id] = self.partialResults[node.id].to_dataset(dim='variable')
                    self.partialResults[node.id].rio.to_raster(self.tmpFolderPath + "/result.tiff")
                    ds = gdal.Open(self.tmpFolderPath + "/result.tiff", gdal.GA_Update)
                    n_of_bands = ds.RasterCount
                    for band in range(n_of_bands):
                        ds.GetRasterBand(band+1).ComputeStatistics(0)
                        ds.GetRasterBand(band+1).SetNoDataValue(np.nan)
                    
                    return 0

                if outFormat.lower() in ['netcdf','nc']:
                    self.outFormat = '.nc'
                    self.mimeType = 'application/octet-stream'
                    logging.info(node.arguments)
                    if 'options' in node.arguments:
                        if 'dtype' in node.arguments['options']:
                            self.partialResults[source] = self.partialResults[source].astype(node.arguments['options']['dtype'])
                        if 'path' in node.arguments['options']:
                            outputFile = node.arguments['options']['path']
                            outputFolder = ""
                            for i in range(1,len(outputFile.split('/'))-1):
                                outputFolder += '/' + outputFile.split('/')[i]
                            if 'mnt' not in outputFolder:
                                raise Exception("[!] Provided output path is not valid!")
                            if os.path.exists(outputFolder):
                                self.tmpFolderPath = outputFile
                                logging.info("New folder " + str(self.tmpFolderPath))
                                self.returnFile = False
                            else:
                                raise Exception("[!] Provided output path is not valid! The folder " + outputFolder + " does not exist!")
                    if 'params' in self.partialResults[source].dims:
                        if self.returnFile:
                            self.partialResults[source].to_netcdf(self.tmpFolderPath + "/result.nc")
                        else:
                            self.partialResults[source].to_netcdf(self.tmpFolderPath)
                        return 0
#                     logging.info('refactor_data')
#                     tmp = self.refactor_data(self.partialResults[source])
                    tmp = self.partialResults[source]
                    tmp = tmp.rio.write_crs(self.crs)
#                     tmp.attrs = self.partialResults[source].attrs
#                     self.partialResults[source].time.encoding['units'] = "seconds since 1970-01-01 00:00:00"
                    try:
                        if self.returnFile:
                            tmp.to_netcdf(self.tmpFolderPath + "/result.nc")
                        else:
                            tmp.to_netcdf(self.tmpFolderPath)
                        return 0
                    except Exception as e:
                        logging.info(e)
                        logging.info("Wrtiting netcdf failed, trying another time....")
                        pass
                    try:
                        tmp.time.attrs.pop('units', None)
                        if self.returnFile:
                            tmp.to_netcdf(self.tmpFolderPath + "/result.nc")
                        else:
                            tmp.to_netcdf(self.tmpFolderPath)
                    except Exception as e:
                        logging.info(e)
                        logging.info("Wrtiting netcdf failed!")
                        pass
#                     del(self.partialResults)
                    return 0
                
                if outFormat.lower() == 'json':
                    self.outFormat = '.json'
                    self.mimeType = 'application/json'
                    if isinstance(self.partialResults[source],gpd.geodataframe.GeoDataFrame):
                        self.partialResults[source].to_file(self.tmpFolderPath + "/result.json", driver="GeoJSON")
                        return
                    else:
                        self.partialResults[node.id] = self.partialResults[source].to_dict()
                        with open(self.tmpFolderPath + "/result.json", 'w') as outfile:
                            json.dump(self.partialResults[node.id],outfile) #indent fix
                        return 

                else:
                    raise Exception("[!] Output format not recognized/implemented: {0}".format(outFormat))
                
                return 0 # Save result is the end of the process graph
            
            logging.info("Elapsed time: {}".format(time() - start_time_proc))
            self.listExecutedIds.append(node.id) # Store the processed nodes ids
            return 1 # Go on and process the next node
        
        except Exception as e:
            logging.info(e)
            raise Exception(processName + '\n' + str(e))
    
    def refactor_data(self,data):
        # The following code is required to recreate a Dataset from the final result as Dataarray, to get a well formatted netCDF
        dataset_list = []
        data.name = None
        for var in data['variable'].values:
            dataset_list.append(data.loc[dict(variable=var)].to_dataset(name=var).drop('variable'))
        tmp = xr.merge(dataset_list)
        tmp.attrs = data.attrs
        return tmp
