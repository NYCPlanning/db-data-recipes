from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from osgeo import ogr
from osgeo import gdal
import requests
from pathlib import Path
import urllib.request
import zipfile


def unzip():
    url = 'https://opendata.arcgis.com/datasets/a36a509eab4e43b4864fb594a35b90d6_0.zip'
    file_path = Path(__file__).parent/'tmp.zip'
    urllib.request.urlretrieve(url, file_path)
    z = zipfile.ZipFile(file_path)
    z.extractall(Path(__file__).parent/'tmp')
    os.system(f'rm {file_path}')

def shp_to_csv():
    sourcePath = Path(__file__).parent
    shapefiles = [sourcePath/'tmp'/each for each in os.listdir(sourcePath/'tmp') if each.endswith('.shp')]
    srcDS = gdal.OpenEx(str(shapefiles[0]))
    gdal.VectorTranslate(
        str(sourcePath.joinpath('tmp/usdot_airports.csv')),
        srcDS,
        format='CSV',
        dstSRS='EPSG:4326',
        options=['-progress'],
        layerCreationOptions=['GEOMETRY=AS_WKT']
    )

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL():
    table_name = 'usdot_airports'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'tmp'/'usdot_airports.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    unzip()
    shp_to_csv()
    ETL()
    clean_up()
