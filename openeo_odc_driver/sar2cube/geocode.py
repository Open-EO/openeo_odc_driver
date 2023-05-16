# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   23/02/2023

import numpy as np
import xarray as xr
import scipy
from scipy.interpolate import griddata
from scipy.spatial import Delaunay
from scipy.interpolate import LinearNDInterpolator, NearestNDInterpolator
from utils import create_S2grid
import sys 
sys.path.append('../')
import log_jobid


_log = log_jobid.LogJobID() 


def geocode(data,parameters,tmp_folder):
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

    if 'resolution' in parameters and parameters['resolution'] is not None:
        spatialres = parameters['resolution']
    else:
        raise Exception("[!] The geocode process is missing the required resolution field.")

    if spatialres not in [10,20,60]:
        raise Exception("[!] The geocode process supports only 10m,20m,60m for resolution to align with the Sentinel-2 grid.")

    if 'crs' in parameters and parameters['crs'] is not None:
        # TODO : move out of here
        self.crs = parameters['crs']
        output_crs = "epsg:" + str(parameters['crs'])
    else:
        raise Exception("[!] The geocode process is missing the required crs field.")

    if 'grid_lon' in data['variable'] and 'grid_lat' in data['variable']:
        pass
    else:
        raise Exception("[!] The geocode process is missing the required grid_lon and grid_lat bands.")
    try: 
        data.loc[dict(variable='grid_lon')]
        data.loc[dict(variable='grid_lat')]
        if len(data.dims) >= 3:
            if 'time' in data.dims and len(data.loc[dict(variable='grid_lon')].dims)>2:
                grid_lon = data.loc[dict(variable='grid_lon',time=data.time[0])].values
                grid_lat = data.loc[dict(variable='grid_lat',time=data.time[0])].values
            else:
                grid_lon = data.loc[dict(variable='grid_lon')].values
                grid_lat = data.loc[dict(variable='grid_lat')].values
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

    _log.debug("Geocoding started!")
    if 'time' in data.dims:
        for t in data['time']:
            geocoded_dataset = None
            data_t = data.loc[dict(time=t)]
            for var in data['variable']:
                if (var.values!='grid_lon' and var.values!='grid_lat'):
                    _log.debug("Geocoding band {} for date {}".format(var.values,t.values))
                    numpy_data = data_t.loc[dict(variable=var)].values
                    print(numpy_data.shape)
                    Parallel(n_jobs=12, verbose=51)(
                        joblibDelayed(chunked_delaunay_interpolation)(index,
                                                                      var.values,
                                                                      tmp_folder,
                                                                      chunks_x_regular[index],
                                                                      chunks_y_regular[index],
                                                                      grid_x_irregular,
                                                                      grid_y_irregular,
                                                                      numpy_data,
                                                                      spatialres,
                                                                      t.values)for index in range(len(chunks_x_regular)))
    else:
        geocoded_dataset = None
        for var in data['variable']:
            if (var.values!='grid_lon' and var.values!='grid_lat'):
                _log.debug("Geocoding band {}".format(var.values))
                numpy_data = data.loc[dict(variable=var)].values
                Parallel(n_jobs=12, verbose=51)(
                    joblibDelayed(chunked_delaunay_interpolation)(index,
                                                                  var.values,
                                                                  tmp_folder,
                                                                  chunks_x_regular[index],
                                                                  chunks_y_regular[index],
                                                                  grid_x_irregular,
                                                                  grid_y_irregular,
                                                                  numpy_data,
                                                                  spatialres)for index in range(len(chunks_x_regular)))
    output = xr.open_mfdataset(tmp_folder + '/*.nc', combine="by_coords").to_array()
    output = output.sortby(output.y)


    tmp_files_to_remove = glob(tmp_folder + '/*.pc')
    Parallel(n_jobs=8)(joblibDelayed(os.remove)(file_to_remove) for file_to_remove in tmp_files_to_remove)
    
    return output