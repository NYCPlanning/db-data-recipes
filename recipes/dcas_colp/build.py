from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3, rename_field, joined_lower
import zipfile
import requests
import shutil
import io

def unzip():
    url = 'https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc-colp-201811.zip'
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall('tmp')

def rm_tmp():
    shutil.rmtree('tmp')

def ETL():

    table_name = 'dcas_colp'
    base_path = create_base_path(__file__)

    Flow(
        load('tmp/COLP_2018.xlsx', name=table_name, format='xlsx', sheet=1),
        rename_field('MAPPABLE (1=NOT MAPPED)', 'MAPPABLE'),
        joined_lower(resources=table_name),
        set_type('bbl', resources=table_name, type='string'),   
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
        ).process()

if __name__ == '__main__':
    unzip()
    ETL()
    rm_tmp()