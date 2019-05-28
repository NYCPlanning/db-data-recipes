from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from osgeo import ogr
from osgeo import gdal
import requests
from pathlib import Path
import urllib.request
import zipfile

def unzip(table_name):
    # this link would need constant update
    url = 'https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyczoningtaxlotdb_20190430.zip'
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
    sourcePath = Path(__file__).parent
    file_path = [filepath for filepath in Path(sourcePath/'tmp').glob('**/*') if filepath.suffix == '.csv'][0]
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'dcp_zoningtaxlots'
    unzip(table_name)
    ETL(table_name)
    clean_up()