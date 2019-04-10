from dataflows import *
from lib import create_csv_path, dump_to_postgis
import os
import csv
import sys
from pathlib import Path
csv.field_size_limit(sys.maxsize)

def ETL():

    path = create_csv_path(Path(__file__).parent)
    url = 'https://sptkl.sfo2.digitaloceanspaces.com/pipelines/db-facilities/2019-04-10/datapackage.json'

    Flow(
        load(url, force_strings=False),
        dump_to_postgis()
    ).process()

if __name__ == '__main__':
    ETL()