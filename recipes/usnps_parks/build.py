from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3, dump_to_postgis
import os 
from pathlib import Path
import gdal 
import sys
import csv
csv.field_size_limit(sys.maxsize)

def download_unzip(): 
    url = 'https://irma.nps.gov/DataStore/DownloadFile/630692'
    path = Path(__file__).parent/'nps_boundry.zip'
    target = Path(__file__).parent/'nps_boundry'
    os.system(f'curl -o {path} {url}')
    os.system(f'unzip {path} -d {target}')

def shp_to_csv():
    root = Path(__file__).parent
    shapefiles = [root/'nps_boundry'/each for each in os.listdir(root/'nps_boundry') if each.endswith('.shp')]
    srcDS = gdal.OpenEx(str(shapefiles[0]))
    gdal.VectorTranslate(
        str(root/'nps_boundry'/'usnps_parks.csv'),
        srcDS,
        format='CSV',
        dstSRS='EPSG:4326',
        options=['-progress'],
        layerCreationOptions=['GEOMETRY=AS_WKT']
    )

def clean_up(): 
    tmp_path = Path(__file__).parent/'nps_boundry'
    zip_path = Path(__file__).parent/'nps_boundry.zip'
    os.system(f'rm -r {tmp_path}')
    os.system(f'rm -r {zip_path}')

def ETL():
    table_name = 'usnps_parks'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'nps_boundry'/'usnps_parks.csv'
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