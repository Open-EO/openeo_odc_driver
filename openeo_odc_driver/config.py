# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   13/09/2024

# Used in openeo_odc_driver/openeo_odc_driver.py:

# Not used currently (local cluster)
DASK_SCHEDULER_ADDRESS = ''

# Remember a slash at the end of this path. Same as the org.openeo.tmp.dir set in openeo-spring-driver if used together.
RESULT_FOLDER_PATH        = '/data/odc-driver/'

# list of available openEO processes. # TODO: this should be internally generated so that we don't rely on external sources.
# OPENEO_PROCESSES       = 'https://gist.githubusercontent.com/clausmichele/7bb949270f031db11e8a78a617144b78/raw/1380bac32442150cddfd70cff23eb79119efd166/test_processes_2.json'
# OPENEO_BACKEND = 'https://dev.openeo.eurac.edu/'
OPENEO_BACKEND = 'https://10.8.244.129:8443/'

# Used in openeo_odc_driver/odc_backend.py and openeo_odc_driver/load_odc_collection.py
# Not necessary if the following environment variables are set:
# DB_HOSTNAME ENV DB_USERNAME ENV DB_PASSWORD ENV DB_DATABASE
OPENDATACUBE_CONFIG_FILE = None

#Used in openeo_odc_driver/odc_backend.py:

# Application name from the Flask web server
FLASK_APP_NAME = 'openeo_odc_driver'

# datacube-explorer endpoint. For local deployment it should be `http://0.0.0.0:9000` and for the Docker deployment `http://explorer:9000`
DATACUBE_EXPLORER_ENDPOINT = 'http://explorer:9000'

USE_CACHED_COLLECTIONS = True
USE_CACHED_PROCESSES = True

LOG_PATH = '/logs/odc-driver/odc_backend.log'
JOB_LOG_FILE = 'jobs_log.csv'

METADATA_FOLDER = './'
METADATA_CACHE_FOLDER = METADATA_FOLDER + 'cache'
METADATA_COLLECTIONS_FILE = METADATA_CACHE_FOLDER + '/' + 'ODC_collections.json'
METADATA_SUPPLEMENTARY_FOLDER = METADATA_FOLDER + 'supplementary'
METADATA_PROCESSES_FILE = METADATA_CACHE_FOLDER + '/' + 'processes.json'

DEFAULT_DATA_PROVIDER = {'name': 'Eurac Research - Institure for Earth Observation', 'url': 'http://www.eurac.edu/', 'roles': ['host']}
DEFAULT_DATA_LICENSE = 'CC-BY-4.0'
DEFAULT_LINKS = [{'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}]
DEFAULT_TEMPORAL_DIMENSION_NAME = 'time'
DEFAULT_X_DIMENSION_NAME = 'x'
DEFAULT_Y_DIMENSION_NAME = 'y'
DEFAULT_BANDS_DIMENSION_NAME = 'bands'

# STAC configuration
POST_RESULTS_TO_STAC = True
STAC_API_URL = "https://stac.openeo.eurac.edu/api/v1/pgstac/collections/"

# OGC GeoDataCube API specific bits
OGC_COVERAGE = True
SUPPORTED_MIME_OGC_COVERAGE = ['application/netcdf','image/tiff; application=geotiff']
DEFAULT_LINK_OGC_COVERAGE = {'rel' : 'http://www.opengis.net/def/rel/ogc/1.0/coverage', 'href' : OPENEO_BACKEND + 'collections/COLLECTION_NAME/coverage', 'type' : 'application/netcdf', 'title' : 'Coverage link'}

#SAR2CUBE specific configs, leave them empty if not required
#Used in openeo_odc_driver/sar2cube/utils.py

LONGITUDE_LAYER_NAME = 'grid_lon'
LATITUDE_LAYER_NAME = 'grid_lat'
S2_FOOTPRINT_FILE = './sar2cube/resources/tabularize_s2_footprint.csv'


# STAC Collections mirrored as openEO Collections

EXTERNAL_STAC_COLLECTION = {
    "SENTINEL2_L2A": {
     "href":"https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
     "assets":[
        "aot",
        "blue",
        "coastal",
        "green",
        "nir",
        "nir08",
        "nir09",
        "red",
        "rededge1",
        "rededge2",
        "rededge3",
        "scl",
        "swir16",
        "swir22",
        "wvp"],
        "properties": None
    },
    "ERA5_REANALYSIS": {
     "href":"https://planetarycomputer.microsoft.com/api/stac/v1/collections/era5-pds",
     "assets":[
        "surface_air_pressure",
        "sea_surface_temperature",
        "eastward_wind_at_10_metres",
        "air_temperature_at_2_metres",
        "eastward_wind_at_100_metres",
        "northward_wind_at_10_metres",
        "northward_wind_at_100_metres",
        "air_pressure_at_mean_sea_level",
        "dew_point_temperature_at_2_metres"],
        "properties": {"era5:kind": {"eq": "an"}}
    },
    "ERA5_FORECAST": {
     "href":"https://planetarycomputer.microsoft.com/api/stac/v1/collections/era5-pds",
     "assets":[
        "precipitation_amount_1hour_Accumulation",
        "air_temperature_at_2_metres_1hour_Maximum",
        "air_temperature_at_2_metres_1hour_Minimum",
        "integral_wrt_time_of_surface_direct_downwelling_shortwave_flux_in_air_1hour_Accumulation"],
        "properties": {"era5:kind": {"eq": "fc"}}
    }
}
