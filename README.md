<p float="center">
  <img src="https://avatars.githubusercontent.com/u/23743223?s=200&v=4" width="200" hspace="20"/>
  <img src="https://avatars.githubusercontent.com/u/26125288?s=200&v=4" width="200" hspace="20"/>
  <img src="https://avatars.githubusercontent.com/u/63704085?s=200&v=4" width="200" hspace="20"/>  
</p>

# OpenEO ODC Driver
OpenEO processing engine written in Python based on OpenDataCube, Xarray and Dask.

Based on openEO processes implemented at [openeo-processes-dask](https://github.com/Open-EO/openeo-processes-dask/) and the parser [openeo-pg-parser-networkx](https://github.com/Open-EO/openeo-pg-parser-networkx).

The `load_collection` process implementation is not part of the openeo-processes-dask repo but it's included here, where it leverages OpenDataCube for data loading: https://github.com/SARScripts/openeo_odc_driver/blob/037a5592c05044d4451d3896c0455414f34dd8f4/openeo_odc_driver/processing.py#L38

# Docker deployment:

Based on the [cube-in-a-box](https://github.com/opendatacube/cube-in-a-box) project and on the Docker image from [andriyreznik](https://github.com/andriyreznik): https://github.com/andriyreznik/docker-python-gdal

It requires Docker and Compose plugin installed. Please check the official documentation if you don't know how to install them: https://docs.docker.com/compose/install/

It also requires `make` to use the Makefile. If you don't have it and/or you can't install it, you can just manually type in the command contained in the Makefile.

## Step 1: Clone the repository
```sh
git clone https://github.com/SARScripts/openeo_odc_driver.git -b dask_processes
cd openeo_odc_driver
```

## Step 2: Create and run the dockers

To create and run the docker containers, run:

```sh
make setup
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

`http://localhost:9001` for the datacube explorer web app

`http://localhost:5001/collections/` for the openEO collections exposed via the openeo_odc_driver.

<details><summary>
  
### Troubleshooting

</summary>
  
#### make setup : `docker.errors.DockerException: Error while fetching server API version`

After checking that the docker service is actually running, this might be then a permissions issue with the socket:

```sh
sudo chmod 666 /var/run/docker.sock
```

(credits: [Mafei@SO](https://stackoverflow.com/a/68179139/1329340))
</details>

## Step 3: Test your environment with an openEO process graph:

```sh
python tests/test_process_graph.py ./tests/process_graphs/NDVI_Bolzano_median.json
```

![image](https://user-images.githubusercontent.com/31700619/220927309-cd4be598-4f93-43cf-ac17-d6dbaa1a2bc3.png)

![image](https://user-images.githubusercontent.com/31700619/220927197-5fccca3a-fff4-4311-9c99-af7c6c4d08f4.png)
