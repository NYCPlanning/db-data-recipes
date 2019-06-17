from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from osgeo import ogr
from osgeo import gdal
import requests
from pathlib import Path
import urllib.request
import zipfile

ftp_prefix = os.environ.get('FTP_PREFIX')

def download(table_name):
    url = ftp_prefix + '/agencysourcedata/acs/acs_facilities_daycareheadstart.csv'
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    os.mkdir(Path(__file__).parent/'tmp')
    urllib.request.urlretrieve(url, file_path)

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(table_name):
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'acs_daycareheadstart'
    download(table_name)
    ETL(table_name)
    clean_up()