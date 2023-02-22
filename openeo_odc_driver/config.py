# Used in openeo_odc_driver/openeo_odc_driver.py:

DASK_SCHEDULER_ADDRESS = "" # Not used currently (local cluster)
RESULT_FOLDER_PATH        = "/tmp/" # Remember a slash at the end of this path. Has to be accessible from all the Dask workers
OPENEO_PROCESSES       = "https://openeo.eurac.edu/processes"

# Used in openeo_odc_driver/odc_backend.py and openeo_odc_driver/load_odc_collection.py
# Not necessary if the following environment variables are set:
# DB_HOSTNAME ENV DB_USERNAME ENV DB_PASSWORD ENV DB_DATABASE

OPENDATACUBE_CONFIG_FILE = None

#Used in openeo_odc_driver/odc_backend.py:

FLASK_APP_NAME = 'openeo_odc_driver'

DATACUBE_EXPLORER_ENDPOINT = "http://explorer:9000"

USE_CACHED_COLLECTIONS = False

JOB_LOG_FILE = 'jobs_log.csv'

METADATA_FOLDER = "./"
METADATA_CACHE_FOLDER = METADATA_FOLDER + "cache"
METADATA_COLLECTIONS_FILE = METADATA_CACHE_FOLDER + "/" + "ODC_collections.json"
METADATA_SUPPLEMENTARY_FOLDER = METADATA_FOLDER + "supplementary"

DEFAULT_DATA_PROVIDER = {'name': 'Eurac EO ODC', 'url': 'http://www.eurac.edu/', 'roles': ['host']}
DEFAULT_DATA_LICENSE = 'CC-BY-4.0'
DEFAULT_LINKS = {'rel' : 'license', 'href' : 'https://creativecommons.org/licenses/by/4.0/', 'type' : 'text/html', 'title' : 'License link'}
DEFAULT_TEMPORAL_DIMENSION_NAME = 'DATE'
DEFAULT_X_DIMENSION_NAME = 'X'
DEFAULT_Y_DIMENSION_NAME = 'Y'
DEFAULT_BANDS_DIMENSION_NAME = 'bands'

#SAR2CUBE specific configs, leave them empty if not required
#Used in openeo_odc_driver/sar2cube/utils.py

LONGITUDE_LAYER_NAME = 'grid_lon'
LATITUDE_LAYER_NAME = 'grid_lat'