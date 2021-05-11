# OpenEO_ODC_Driver
OpenEO process graph executor written in Python based on OpenDataCube, Xarray and Dask

<p float="left">
  <img src="/img/openeo_logo.png" width="200" />
  <img src="/img/sar2cube.png" width="300" /> 
</p>

# Installation
## Step 1: Clone the repository
```
git clone https://github.com/SARScripts/openeo_odc_driver.git
cd openeo_odc_driver
```
## Step 2: Prepare the python environment
```
conda env create -f environment.yml
conda activate openeo_odc_driver
git clone https://github.com/clausmichele/openeo-pg-parser-python.git
cd openeo-pg-parser-python
pip install .
```

## Step 3:
Modify the code with your system's details:
1. In [odc_backend.py](https://github.com/SARScripts/openeo_odc_driver/blob/master/odc_backend.py) you have to insert the datacube-explorer address and the OpenDatcube config file path:
```
DATACUBE_EXPLORER_ENDPOINT = "http://0.0.0.0:9000"
OPENDATACUBE_CONFIG_FILE = ""
```
2. In [odc_wrapper.py](https://github.com/SARScripts/openeo_odc_driver/blob/master/odc_wrapper.py) you have to insert the OpenDatcube config file path:
```
OPENDATACUBE_CONFIG_FILE = ""
```
3. In [openeo_odc_driver.py](https://github.com/SARScripts/openeo_odc_driver/blob/master/openeo_odc_driver.py) you have to insert the Dask Scheduler address and the tmp folder to write output files:
```
DASK_SCHEDULER_ADDRESS = ''
TMP_FOLDER_PATH        = '' # Has to be accessible from all the Dask workers
```
If the environment creation step fails please create a Python 3.7 environment environment with the following libraries:
gdal, xarray, rioxarray, dask, numpy, scipy, opencv and their dependencies.
## Step 4: Start the web server:
```
gunicorn -c gunicorn.conf.py odc_backend:app
```


# Implemented OpenEO processes
## aggregate & resample
- resample_cube_temporal
- resample_spatial
## arrays
- array_element
## climatology
- climatological_normal (only monthly frquency at the moment)
- anomaly (only monthly frquency at the moment)
## comparison
- if
- lt
- lte
- gt
- gte
- eq
- neq
## cubes
- load_collection
- save_result (PNG,GTIFF,NETCDF)
- reduce_dimension (dimensions: t (or temporal), bands)
- filter_bands
- filter_temporal
- rename_labels
- merge_cubes
- apply
## logic
- and
- or
## masks
- mask
## math
- multiply
- divide
- subtract
- add
- sum
- product
- sqrt
- normalized_difference
- min
- max
- mean
- median
- power
- absolute
- linear_scale_range
## experimental processes
- aggregate_spatial_window







