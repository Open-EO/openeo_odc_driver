# coding=utf-8
# Author: Claus Michele - Eurac Research - michele (dot) claus (at) eurac (dot) edu
# Date:   03/07/2020

import dask
from dask.distributed import Client
from openeo_odc_driver import OpenEO
import argparse
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("jsonGraph", help="The process graph path. Example: ./process_graphs/test.json")
parser.add_argument("--local", type=int, default=0, help="Perform local test without opendatacube, default value is 0")
args = parser.parse_args()

if __name__ == '__main__':
    if not os.path.isfile(args.jsonGraph):
        print("[!] Please provide the path of an existing json process graph.")
        sys.exit(0)
        
    client = Client() # Create a Dask client and worker
    
    eo = OpenEO(args.jsonGraph,args.local)