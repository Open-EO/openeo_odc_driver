# OpenEO_ODC_Driver
OpenEO backend written in Python based on OpenDataCube

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

