# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   08/02/2022

import datacube
import numpy as np
import xarray as xr
import copy
from datetime import datetime
from time import time
import shapely
from shapely.geometry import shape
#libraries for polygon and polygon mask
import fiona
import shapely.geometry
import rasterio
from datacube.utils import geometry
from datacube.utils.geometry import Geometry, CRS
import dea_tools.datahandling  # or some other submodule
from config import *
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("odc_openeo_engine.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

class Odc:
    def __init__(self,collections=None,timeStart=None,timeEnd=None,south=None,north=None,west=None,east=None,bands=None,resolutions=None,outputCrs=None,polygon=None,resamplingMethod=None,crs=None):

        self.dc = datacube.Datacube(config = OPENDATACUBE_CONFIG_FILE)
        self.collections = collections
        self.timeStart   = timeStart
        self.timeEnd     = self.exclusive_date(timeEnd)
        self.south       = south
        self.north       = north
        self.west        = west
        self.east        = east
        self.bands       = bands
        self.resolutions = resolutions
        self.outputCrs   = outputCrs
        self.resamplingMethod = resamplingMethod
        self.polygon     = polygon
        self.geoms       = None
        self.crs         = crs
        self.data        = None
        self.query       = None
        self.build_query()
        self.load_collection()
        if self.polygon is not None: # We mask the data with the given polygon, i.e. we set to zero the values outside the polygon
            self.apply_mask()
    
    def sar2cube_collection(self):
        return ('SAR2Cube' in self.collections) # Return True if it's a SAR2Cube collection, where spatial subsetting can't be performed in the usual way
    
    def exclusive_date(self,date):
        return str(np.datetime64(date) - np.timedelta64(1, 'D')).split(' ')[0] # Substracts one day
        
    def build_query(self):
        query = {}
        query['product'] = self.collections
        if self.bands is not None:
            query['measurements'] = self.bands
        if self.polygon is not None:
            #crs = CRS("epsg:4326")
            #geom = Geometry(geom=self.polygon, crs=crs)
            #query['geopolygon'] = geom
            self.get_bbox()
        if (self.south is not None and self.north is not None and self.east is not None and self.west is not None and not self.sar2cube_collection()):
            if self.crs is not None:
                query['crs']  = 'epsg:' + str(self.crs)
                query['x']  = (self.south,self.north)
                query['y'] = (self.east,self.west)
                query['output_crs'] = 'epsg:' + str(self.crs)
                query['resolution'] = [10,10]
            else:
                query['latitude']  = (self.south,self.north)
                query['longitude'] = (self.east,self.west)
        if self.resolutions is not None:
            query['resolution'] = self.resolutions
        if self.outputCrs  is not None:
            query['output_crs'] = self.outputCrs
        self.query = query
        
    def load_collection(self):
        datasets  = self.dc.find_datasets(time=(self.timeStart,self.timeEnd),**self.query)
        self.query['dask_chunks'] = {"time":1,"x": 1000, "y":1000}             # This let us load the data as Dask chunks instead of numpy arrays
        if self.resamplingMethod  is not None:
            if self.resamplingMethod == 'near':
                self.query['resampling'] = 'nearest'
            else:
                ##TODO add other method parsing here
                self.query['resampling'] = self.resamplingMethod
        
        try:
            self.data = self.dc.load(datasets=datasets,**self.query).astype(np.float32)
            if self.data.equals(xr.Dataset()):
                raise Exception("load_collection returned an empty dataset, please check the requested bands, spatial and temporal extent.")
        except Exception as e:
            if (str(e)=='Product has no default CRS. Must specify \'output_crs\' and \'resolution\''):
                # Identify the most common projection system in the input query
                crs_query = copy.deepcopy(self.query)
                crs_query.pop('product')
                crs_query.pop('dask_chunks')
                output_crs = dea_tools.datahandling.mostcommon_crs(dc=self.dc, product=self.collections, query=crs_query)
                print(output_crs)
                self.query['output_crs'] = output_crs
                self.query['resolution'] = [10,10]
                self.query['dask_chunks'] = {"time":1,"x": 1000, "y":1000}
                self.data = self.dc.load(datasets=datasets,**self.query)
            else:
                raise e

            
        if (self.sar2cube_collection() and self.south is not None and self.north is not None and self.east is not None and self.west is not None):
            attrs = self.data.attrs
            start_masking = time()    
            bbox = [self.west,self.south,self.east,self.north]
            grid_lon = self.data.grid_lon[0]
            grid_lat = self.data.grid_lat[0]
            bbox_mask = np.bitwise_and(np.bitwise_and(grid_lon>bbox[0],grid_lon<bbox[2]),np.bitwise_and(grid_lat>bbox[1],grid_lat<bbox[3]))
            # self.data = self.data.where(bbox_mask,drop=True)
            bbox_mask = bbox_mask.where(bbox_mask,drop=True)
            self.data = self.data * bbox_mask
            self.data.attrs = attrs
        if self.sar2cube_collection():
            self.data['grid_lon'] = self.data.grid_lon.where(self.data.grid_lon!=0)
            self.data['grid_lat'] = self.data.grid_lat.where(self.data.grid_lat!=0)
            logging.info("Elapsed time data masking: {}".format(time() - start_masking))
            
    def list_measurements(self):   # Get all the bands available in the loaded data as a list of strings
        measurements = []
        content = str(self.data)
        meas = []
        lines = content.split('Data variables:')[1].split('Attributes:')[0].splitlines()[1:]
        for line in lines:
            meas.append(line.split('  (time')[0].replace(" ", ""))
        measurements.append(meas)
        return measurements
    
    def build_geometry_fromshapefile(self):
        shapes = fiona.open(self.polygon)
        print('Number of shapes in ',self.polygon,' :',len(shapes))
        print('crs ',shapes.crs['init'])
        #copy attributes from shapefile and define shape_name
        geoms = []
        for i in range(len(shapes)):
            geom_crs = geometry.CRS(shapes.crs['init'])
            geo = shapes[i]['geometry']
            geom = geometry.Geometry(geo, crs=geom_crs)
            geoms.append(geom)
            #geom_bs = shapely.geometry.shape(shapes[i]['geometry'])
        #shape_name = shape_file.split('/')[-1].split('.')[0]+'_'+str(i)
        return geoms    
    
    def get_bbox(self):
        self.south      = np.min([[el[1] for el in self.polygon[0]]])
        self.north      = np.max([[el[1] for el in self.polygon[0]]])
        self.east      = np.min([[el[0] for el in self.polygon[0]]])
        self.west     = np.max([[el[0] for el in self.polygon[0]]])
        return
    
    def apply_mask(self):
        geoms = []
        pol = {}
        pol['type'] = 'Polygon'
        coords = [[(el[0], el[1]) for el in self.polygon[0]]]
        pol['coordinates'] = coords
        geom = geometry.Geometry(pol, crs='epsg:4326')
        geoms.append(geom)
        mask = self.geometry_mask(geoms, self.data.geobox, invert=True)
        self.data = self.data.where(mask)
        return

    def geometry_mask(self, geoms, geobox, all_touched=False, invert=False):
        """
        Create a mask from shapes.

        By default, mask is intended for use as a
        numpy mask, where pixels that overlap shapes are False.
        :param list[Geometry] geoms: geometries to be rasterized
        :param datacube.utils.GeoBox geobox:
        :param bool all_touched: If True, all pixels touched by geometries will be burned in. If
                                 false, only pixels whose center is within the polygon or that
                                 are selected by Bresenham's line algorithm will be burned in.
        :param bool invert: If True, mask will be True for pixels that overlap shapes.
        """
        return rasterio.features.geometry_mask([geom.to_crs(geobox.crs) for geom in geoms],
                                               out_shape=geobox.shape,
                                               transform=geobox.affine,
                                               all_touched=all_touched,
                                               invert=invert)

 