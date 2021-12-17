from pyproj import Proj, transform, Transformer, CRS
import numpy as np
import os
import pandas as pd
import sys
import xarray as xr
from scipy import spatial

def check_S2grid(ulx, uly, lrx, lry,epsgout,spatialres):
    #Extract x,y reference coordinates for each zone
    s2gridpath = './tabularize_s2_footprint.csv'
    if not os.path.isfile(s2gridpath):
        print('Sentinel grid file not found in /RES')
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
        print('Sentinel grid file provided not coherent with coordinates ')
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
    print('Irregular grid bounds: ',min_x, min_y, max_x, max_y)
    output_crs_int = int(output_crs.split(':')[1])
    print("OUTPUT CRS: ",output_crs_int)
    ulxgrid, ulygrid, lrxgrid, lrygrid = check_S2grid(min_x, max_y, max_x, min_y,output_crs_int,spatialres)
    print('Aligned with S2 grid bounds: ',ulxgrid, lrygrid, lrxgrid, ulygrid)
    wout = int((lrxgrid - ulxgrid) / spatialres)
    hout = int((ulygrid - lrygrid) / spatialres)
    x_regular = np.linspace(ulxgrid, lrxgrid, int(wout+1)) + 10
    y_regular = np.linspace(lrygrid, ulygrid, int(hout+1)) + 10
    return x_regular.astype(np.float32), y_regular.astype(np.float32), grid_x_irregular.astype(np.float32), grid_y_irregular.astype(np.float32)

def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return array[idx]

def find_closest_2d(datacube,points):
    lon_lat = np.asarray([datacube.grid_lon[0].fillna(0).values.flatten(), datacube.grid_lat[0].fillna(0).values.flatten()]).T
    tree = spatial.KDTree(lon_lat)
    res = tree.query(points)
    found_point = lon_lat[res[1]]
    x_ml = sent1_ML.grid_lat[0].where(sent1_ML.grid_lat[0]==found_point[1],drop=True).x.values
    y_ml = sent1_ML.grid_lat[0].where(sent1_ML.grid_lat[0]==found_point[1],drop=True).y.values
    return x_ml, y_ml