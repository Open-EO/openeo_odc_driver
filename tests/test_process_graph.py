# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   23/02/2023

import requests
import json
import sys
from time import time
from openeo_odc_driver.config import *

start = time()
if len(sys.argv) != 2:
    raise ValueError('Please provide process graph.')

process_graph = sys.argv[1]
with open(process_graph) as f:
    d = json.load(f)
res = requests.post('http://0.0.0.0:5001/graph', json=d)
print(res)
print(res.headers['content-type'])
print('Result stored in the Docker folder ',RESULT_FOLDER_PATH + res.json()['output'])
print('Elapsed time: ',time() - start)