from pyproj import Proj, transform, Transformer, CRS
import numpy as np
import os
import pandas as pd
import sys
import xarray as xr
from scipy import spatial
import logging
from openeo_odc_driver.config import LATITUDE_LAYER_NAME, LONGITUDE_LAYER_NAME

_log = logging.getLogger(__name__)

def check_S2grid(ulx, uly, lrx, lry,epsgout,spatialres):
    #Extract x,y reference coordinates for each zone
    s2gridpath = './resources/tabularize_s2_footprint.csv'
    if not os.path.isfile(s2gridpath):
        _log.error('Sentinel grid file tabularize_s2_footprint.csv not found in /resources')
        sys.exit()
        #load s2gridpath
    s2grid = pd.read_csv(s2gridpath, sep=',')
    s2grid = s2grid.drop(np.where(s2grid.epsg != epsgout)[0])
    s2grid = s2grid.reset_index()
    epsglist = set(s2grid['epsg'])
    #Extract unique zones
    s2grid['zone_ref'] = [Proj(s2grid['utm_zone'][i]).crs.utm_zone for i in range(len(s2grid))]

    if all(s2grid.groupby('zone_ref')['ul_x'].apply(lambda x: len(set(x%float(spatialres)))==1)):
        s2gridrefx = {ep:s2grid[s2grid['epsg']==ep].iloc[0]['ul_x'] for ep in epsglist}
        s2gridrefy = {ep:s2grid[s2grid['epsg']==ep].iloc[0]['ul_y'] for ep in epsglist}
    else:
        _log.error('Sentinel grid file provided not coherent with coordinates ')
        sys.exit()
    if s2gridrefx[int(epsgout)] > ulx:
        ulxgrid = s2gridrefx[int(epsgout)] - (int((s2gridrefx[float(epsgout)] - ulx) / spatialres)*spatialres) + spatialres
    else:
        ulxgrid = s2gridrefx[int(epsgout)] - int((s2gridrefx[float(epsgout)] - ulx) / spatialres)*spatialres
    if s2gridrefy[int(epsgout)] > uly:
        ulygrid = s2gridrefy[int(epsgout)] - int((s2gridrefy[float(epsgout)] - uly) / spatialres)*spatialres
    else:
        ulygrid = s2gridrefy[int(epsgout)] - (int((s2gridrefy[float(epsgout)] - uly) / spatialres)*spatialres - spatialres)

    if s2gridrefx[int(epsgout)] >= lrx:
        lrxgrid = s2gridrefx[int(epsgout)] - int((s2gridrefx[float(epsgout)] - lrx) / spatialres)*spatialres
    else:
        lrxgrid = s2gridrefx[int(epsgout)] - (int((s2gridrefx[float(epsgout)] - lrx) / spatialres)*spatialres - spatialres)
    if s2gridrefy[int(epsgout)] > lry:
        lrygrid = s2gridrefy[int(epsgout)] - (int((s2gridrefy[float(epsgout)] - lry) / spatialres)*spatialres) + spatialres
    else:
        lrygrid = s2gridrefy[int(epsgout)] - int((s2gridrefy[float(epsgout)] - lry) / spatialres)*spatialres
    return (ulxgrid, ulygrid, lrxgrid, lrygrid)

def create_S2grid(grid_lon,grid_lat,output_crs,spatialres):
    transformer = Transformer.from_crs("epsg:4326", output_crs)
    grid_x_irregular,grid_y_irregular = transformer.transform(grid_lat,grid_lon)
    min_x = np.nanmin(grid_x_irregular)
    min_y = np.nanmin(grid_y_irregular)
    grid_x_irregular[grid_x_irregular==np.inf] = 0
    grid_y_irregular[grid_y_irregular==np.inf] = 0
    max_x = np.nanmax(grid_x_irregular)
    max_y = np.nanmax(grid_y_irregular)
    _log.debug('Irregular grid bounds: ',min_x, min_y, max_x, max_y)
    output_crs_int = int(output_crs.split(':')[1])
    _log.debug('Output CRS: ',output_crs_int)
    ulxgrid, ulygrid, lrxgrid, lrygrid = check_S2grid(min_x, max_y, max_x, min_y,output_crs_int,spatialres)
    _log.debug('Aligned with S2 grid bounds: ',ulxgrid, lrygrid, lrxgrid, ulygrid)
    wout = int((lrxgrid - ulxgrid) / spatialres)
    hout = int((ulygrid - lrygrid) / spatialres)
    if spatialres==10:
        x_regular = np.linspace(ulxgrid + 5, lrxgrid - 5, int(wout))
        y_regular = np.linspace(ulygrid - 5, lrygrid + 5, int(hout))
    elif spatialres==20:
        x_regular = np.linspace(ulxgrid + 10, lrxgrid - 10, int(wout))
        y_regular = np.linspace(ulygrid - 10, lrygrid + 10, int(hout))
    else:
        x_regular = np.linspace(ulxgrid + 30, lrxgrid - 30, int(wout))
        y_regular = np.linspace(ulygrid - 30, lrygrid + 30, int(hout))
    
    return x_regular.astype(np.float32), y_regular.astype(np.float32), grid_x_irregular.astype(np.float32), grid_y_irregular.astype(np.float32)

def sar2cube_collection_extent(collectionName):
    dc = datacube.Datacube(config = OPENDATACUBE_CONFIG_FILE)
    sar2cubeData = dc.load(product = collectionName, dask_chunks={'time':1,'x':2000,'y':2000})
    zero_lon_mask = sar2cubeData[LONGITUDE_LAYER_NAME][0]!=0
    zero_lat_mask = sar2cubeData[LATITUDE_LAYER_NAME][0]!=0
    min_lon = sar2cubeData[LONGITUDE_LAYER_NAME][0].where(zero_lon_mask).min().values.item(0)
    min_lat = sar2cubeData[LATITUDE_LAYER_NAME][0].where(zero_lat_mask).min().values.item(0)
    max_lon = sar2cubeData[LONGITUDE_LAYER_NAME][0].where(zero_lon_mask).max().values.item(0)
    max_lat = sar2cubeData[LATITUDE_LAYER_NAME][0].where(zero_lat_mask).max().values.item(0)
    return [min_lon,min_lat,max_lon,max_lat]