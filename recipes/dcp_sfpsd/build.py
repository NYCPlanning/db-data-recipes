from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from osgeo import ogr
from osgeo import gdal
import requests
from pathlib import Path
import urllib.request
import zipfile
import pandas as pd

def unzip(table_name):
    url = 'https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/facilities_csv_201901.zip'
    file_path = Path(__file__).parent/f'{table_name}.zip'
    urllib.request.urlretrieve(url, file_path)
    z = zipfile.ZipFile(file_path)
    z.extractall(Path(__file__).parent/'tmp')
    os.system(f'rm {file_path}')

def clean_up():
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(table_name):
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'dcp_sfpsd.csv'

    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'dcp_sfpsd'
    unzip(table_name)
    file_path = Path(__file__).parent/'tmp'/'facilities_20190110.csv'
    df = pd.read_csv(file_path, dtype = 'str')
    dcp_sfpsd = ['amtrak_facilities_sfpsd', 'bbpc_facilities_sfpsd',
                'hrpt_facilities_sfpsd', 'mta_facilities_sfpsd',
                'nysdot_facilities_sfpsd', 'panynj_facilities_sfpsd',
                'tgi_facilities_sfpsd', 'dcas_facilities_colp',
                'rioc_facilities_sfpsd'] 
    df = df[df.pgtable.isin(dcp_sfpsd)]
    df.to_csv(Path(__file__).parent/'dcp_sfpsd.csv')
    ETL(table_name)
    clean_up()