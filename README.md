# OpenEO_ODC_Driver
OpenEO backend written in Python based on OpenDataCube, xarray and dask

## Step 1: Clone the repository
```
git clone https://github.com/SARScripts/openeo_odc_driver.git
cd openeo_odc_driver
```
## Step 2: Prepare the python environment
```
conda env create -f environment.yml
conda activate openeo_odc_driver
git clone https://github.com/Open-EO/openeo-pg-parser-python.git
python openeo-pg-parser-python/setup.py install
```
If the environment creation step fails please create a Python 3.7 environment environment with the following libraries:
gdal, xarray, rioxarray, dask, numpy, scipy, opencv and their dependencies.
## Step 3: Test with local datacube
```
python main.py ./process_graphs/EVI_L1C_D22.json --local 1
```

## Implemented OpenEO processes

- load_collection
- save_result (PNG,GTIFF,NETCDF)
- resample_spatial
- multiply
- divide
- subtract
- add
- lt
- lte
- gt
- gte
- eq
- neq
- and
- or
- sum
- product
- sqrt
- array_element
- normalized_difference
- reduce_dimension (dimensions: t (or temporal), bands)
- min
- max
- mean
- median
- power
- absolute
- linear_scale_range
- filter_bands
- filter_temporal
- rename_labels
- merge_cubes
- apply
- mask

# Experimetnal processes
- resample_cube_temporal
- aggregate_spatial_window




