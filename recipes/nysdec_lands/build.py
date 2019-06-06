from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3, dump_to_postgis
import os 
from pathlib import Path
import gdal 
import sys
import csv
csv.field_size_limit(sys.maxsize)

def download_unzip(): 
    url = 'http://gis.ny.gov/gisdata/data/ds_1114/DEC_lands.zip'
    path = Path(__file__).parent/'DEC_lands.zip'
    target = Path(__file__).parent/'tmp'
    os.system(f'curl -o {path} {url}')
    os.system(f'unzip {path} -d {target}')

def shp_to_csv():
    root = Path(__file__).parent
    shapefiles = [root/'tmp'/each for each in os.listdir(root/'tmp') if each.endswith('.shp')]
    srcDS = gdal.OpenEx(str(shapefiles[0]))
    gdal.VectorTranslate(
        str(root/'tmp'/'nysdec_lands.csv'),
        srcDS,
        format='CSV',
        dstSRS='EPSG:4326',
        options=['-progress'],
        layerCreationOptions=['GEOMETRY=AS_WKT']
    )

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    zip_path = Path(__file__).parent/'DEC_lands.zip'
    os.system(f'rm -r {tmp_path}')
    os.system(f'rm -r {zip_path}')

def ETL():
    table_name = 'nysdec_lands'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    download_unzip()
    shp_to_csv()
    ETL()
    clean_up()