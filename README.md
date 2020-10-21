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
git clone https://github.com/clausmichele/openeo-pg-parser-python.git
cd openeo-pg-parser-python
pip install .
```
If the environment creation step fails please create a Python 3.7 environment environment with the following libraries:
gdal, xarray, rioxarray, dask, numpy, scipy, opencv and their dependencies.
## Step 3: Test with local datacube
```
python main.py ./process_graphs/EVI_L1C_D22.json --local 1
```

## Implemented OpenEO processes
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







