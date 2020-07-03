# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   03/07/2020

import dask
from dask.distributed import Client
from openeo_odc_driver import OpenEO

if __name__ == '__main__':
    client = Client() # Create a Dask client and worker
    
    pg_filepath = "./process_graphs/merge_cubes3.json"

    eo = OpenEO(pg_filepath)