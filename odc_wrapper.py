# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   03/07/2020

import datacube
import numpy as np
from datetime import datetime
import shapely
from shapely.geometry import shape
#libraries for polygon and polygon mask
import fiona
import shapely.geometry
import rasterio.features
import rasterio
from datacube.utils import geometry
from datacube.drivers.netcdf import write_dataset_to_netcdf

class Odc:
    def __init__(self,collections=None,timeStart=None,timeEnd=None,lowLat=None,\
                 highLat=None,lowLon=None,highLon=None,bands=None,resolutions=None,output_crs=None,polygon=None):

        self.dc = datacube.Datacube(app = "Sentinel", config = '/home/mclaus@eurac.edu/.datacube.conf')
        #self.platform = 'SENTINEL_2A'
        self.collections = collections
        self.timeStart   = timeStart
        self.timeEnd     = self.exclusive_date(timeEnd)
        self.lowLat      = lowLat
        self.highLat     = highLat
        self.lowLon      = lowLon
        self.highLon     = highLon
        self.bands       = bands
        self.resolutions = resolutions
        self.output_crs  = output_crs
        self.polygon     = polygon
        self.geoms       = None
        self.data        = None
        self.query       = None
        self.build_query()
        self.load_collection()
        if self.polygon is not None: # We mask the data with the given polygon, i.e. we set to zero the values outside the polygon
            self.apply_mask()
    
    def exclusive_date(self,date):
        return str(np.datetime64(date) - np.timedelta64(1, 'D')).split(' ')[0] # Substracts one day
        
    def build_query(self):
        query = {}
        query['product'] = self.collections
        if self.bands is not None:
            query['measurements'] = self.bands
        if self.polygon is not None:
            self.get_bbox()
        if (self.lowLat is not None and self.highLat is not None and self.lowLon is not None and self.highLon is not None):
            query['latitude']  = (self.lowLat,self.highLat)
            query['longitude'] = (self.lowLon,self.highLon)
        if self.resolutions is not None:
            query['resolution'] = self.resolutions
        if self.output_crs  is not None:
            query['output_crs'] = self.output_crs
        print(query)
        self.query = query
            
    def load_collection(self):
        datasets  = self.dc.find_datasets(time=(self.timeStart,self.timeEnd),**self.query)
        self.query['dask_chunks'] = {}                                                      # This let us load the data as Dask chunks instead of numpy arrays
        self.data = self.dc.load(datasets=datasets,**self.query)
    
    def list_measurements(self):                                                            # Get all the bands available in the loaded data as a list of strings
        measurements = []
        content = str(self.data)
        meas = []
        lines = content.split('Data variables:')[1].split('Attributes:')[0].splitlines()[1:]
        for line in lines:
            meas.append(line.split('  (time')[0].replace(" ", ""))
        measurements.append(meas)
        return measurements
    
    def get_min(self):
        list_min = []
        bands = self.list_measurements()
        times = self.data.time.values
        for j in range(len(times)):
            band_min = []
            for k in range(len(bands)):
                band_min.append(np.min(self.data[bands[k]].values))
            list_min.append({'time': str(self.data.time.values[j]), 'min':min(band_min)})
        return list_min
    
    def get_max(self):
        list_max = []
        bands = self.list_measurements()
        times = self.data.time.values
        for j in range(len(times)):
            band_max = []
            for k in range(len(bands)):
                band_max.append(np.max(self.data[bands[k]].values))
            list_max.append({'time': str(self.data.time.values[j]), 'max':max(band_max)})
        return list_max
    
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
        self.lowLat      = np.min([[el[1] for el in self.polygon[0]]])
        self.highLat     = np.max([[el[1] for el in self.polygon[0]]])
        self.lowLon      = np.min([[el[0] for el in self.polygon[0]]])
        self.highLon     = np.max([[el[0] for el in self.polygon[0]]])
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

 