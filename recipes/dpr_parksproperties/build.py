from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from osgeo import ogr
from osgeo import gdal
import requests
from pathlib import Path
import urllib.request
import zipfile
import csv
import sys
csv.field_size_limit(sys.maxsize)


def unzip():
    url = 'https://data.cityofnewyork.us/api/geospatial/k2ya-ucmv?method=export&format=Shapefile'
    file_path = Path(__file__).parent/'tmp.zip'
    urllib.request.urlretrieve(url, file_path)
    z = zipfile.ZipFile(file_path)
    z.extractall(Path(__file__).parent/'tmp')
    os.system(f'rm {file_path}')

def shp_to_csv(table_name):
    sourcePath = Path(__file__).parent
    shapefiles = [sourcePath/'tmp'/each for each in os.listdir(sourcePath/'tmp') if each.endswith('.shp')]
    srcDS = gdal.OpenEx(str(shapefiles[0]))
    gdal.VectorTranslate(
        str(sourcePath.joinpath(f'tmp/{table_name}.csv')),
        srcDS,
        format='CSV',
        dstSRS='EPSG:4326',
        options=['-progress'],
        layerCreationOptions=['GEOMETRY=AS_WKT']
    )

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
    table_name = 'dpr_parksproperties'
    unzip()
    shp_to_csv(table_name)
    ETL(table_name)
    clean_up()
