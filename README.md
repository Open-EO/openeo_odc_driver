# OpenEO ODC Driver
OpenEO processing engine written in Python based on OpenDataCube, Xarray and Dask.

**Please note: this project is still under development and many changes will occur in the next months.**

Currently it is based on openEO processes implemented in this repository together with the openEO process graph parser from https://github.com/Open-EO/openeo-pg-parser-python

The next phase of the project will use the process implementations available at [openeo-processes-dask](https://github.com/Open-EO/openeo-processes-dask/) and the parser [openeo-pg-parser-networkx](https://github.com/Open-EO/openeo-pg-parser-networkx).

<p float="center">
  <img src="https://avatars.githubusercontent.com/u/23743223?s=200&v=4" width="200" hspace="20"/>
  <img src="https://avatars.githubusercontent.com/u/26125288?s=200&v=4" width="200" hspace="20"/>
  <img src="https://avatars.githubusercontent.com/u/63704085?s=200&v=4" width="200" hspace="20"/>  
</p>

# Docker deployment:

Based on the [cube-in-a-box](https://github.com/opendatacube/cube-in-a-box) project and on the Docker image from [andriyreznik](https://github.com/andriyreznik): https://github.com/andriyreznik/docker-python-gdal

It requires Docker and Compose plugin installed. Please check the official documentation if you don't know how to install them: https://docs.docker.com/compose/install/

It also requires `make` to use the Makefile. If you don't have it and/or you can't install it, you can just manually type in the command contained in the Makefile.

## Step 1: Clone the repository
```sh
git clone https://github.com/SARScripts/openeo_odc_driver.git -b dev
cd openeo_odc_driver
```

## Step 2: Create and run the dockers

To create and run the docker containers, run:

```sh
sudo make setup
```

This step will run in a sequence the Makefile steps `build up init product index explorer`. The result will be 3 dockers running.

It will automatically index publicly available Sentinel-2 data. To change the area of interest, modify the BBOX in the Makefile: `lon_min,lat_min,lon_max,lat_max`

One will serve the postgres database containing the OpenDataCube index data.

Another will serve the `datacube-explorer` web application, exposing the OpenDataCube metadata.

The last one will serve the `openeo_odc_driver`, getting as input openEO proces graphs and processing them.

If everything went well, you should see them running using:

```sh
sudo docker ps -a
CONTAINER ID        IMAGE                               COMMAND                  CREATED             STATUS                    PORTS                    NAMES
220ebc8a1dfe        openeoodcdriver_openeo_odc_driver   "/tini -- gunicorn -…"   18 hours ago        Up 18 hours               0.0.0.0:5001->5000/tcp   openeoodcdriver_openeo_odc_driver_1
c6a419dfb99d        openeoodcdriver_explorer            "/tini -- gunicorn '…"   18 hours ago        Up 18 hours               0.0.0.0:9001->9000/tcp   openeoodcdriver_explorer_1
fa1b162c8d44        postgis/postgis:12-2.5              "docker-entrypoint.s…"   36 hours ago        Up 36 hours               0.0.0.0:5433->5432/tcp   openeoodcdriver_postgres_1
```

You can verify that the deployment was successfull visiting:

`https://localhost:9001` for the datacube explorer web app

`https://localhost:5001/collections/` for the openEO collections exposed via the openeo_odc_driver.

# Local installation instructions:

## Step 1: Clone the repository
```sh
git clone https://github.com/SARScripts/openeo_odc_driver.git
cd openeo_odc_driver
```

## Step 2: Prepare the python environment

New ad-hoc conda environment:
```sh
conda env create -f environment.yml
conda activate openeo_odc_driver
```

## Step 3:
Modify the `config.py` file with your system's details:
1. Set the datacube-explorer address. For local deployment it should be `http://0.0.0.0:9000` and for the Docker deployment `http://explorer:9000`.
```python
DATACUBE_EXPLORER_ENDPOINT = "http://0.0.0.0:9000"
```

2. Set the OpenDatCube config file `.datacube.conf` path or leave it to None if ENV variables are set (like in the Docker deployment).
```python
OPENDATACUBE_CONFIG_FILE = ~/.datacube.conf # or None
```

3. Set the result folder path to write output files. If this application is used together with the [openeo-sping-driver](https://github.com/Open-EO/openeo-spring-driver), used for serving the full openEO API, this folder should be the same as the one set in `application.properties` for `org.openeo.tmp.dir`, so that the `openeo-spring-driver` can read the result directly from there.
```python
RESULT_FOLDER_PATH = ''
```

4. The `OPENEO_PROCESSES` variable is used to retrieve the list of available openEO processes. It can be the path to a json file, a dictionaty or an http address. See [here](https://github.com/Open-EO/openeo-pg-parser-python/blob/798668e461ec2a0d3153873413afb0a76a72b61a/src/openeo_pg_parser/translate.py#L263) for detailed info. The dault value is the /processes endpoint of the Eurac openEO back-end.
```python
OPENEO_PROCESSES = "https://openeo.eurac.edu/processes"
```

The other config parameters could be looked at later on and are not affecting the `openeo_odc_driver` functionality. In the config.py file there are comments explaining their usage.

## Step 4: Start the web server:
```sh
gunicorn -c gunicorn.conf.py odc_backend:app
```

# Implemented OpenEO processes (to be updated)
## aggregate & resample
- resample_cube_temporal
- resample_cube_spatial
- aggregate_spatial
- aggregate_spatial_window
- aggregate_temporal_period
## arrays
- array_element
- array_interpolate_linear
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
- save_result (PNG,GTIFF,NETCDF,JSON)
- reduce_dimension
- add_dimension
- apply_dimension
- filter_bands
- filter_temporal
- filter_spatial
- filter_bbox
- rename_labels
- merge_cubes
- apply
- fit_curve
- predict_curve
- resample_cube_spatial
- resample_cube_temporal 
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
- log
- ln
- quantiles
- clip
## experimental processes (SAR2Cube)
- coherence
- geocoding
- radar_mask
