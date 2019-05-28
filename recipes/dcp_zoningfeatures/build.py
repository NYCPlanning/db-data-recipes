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

ftp_prefix = os.environ.get('FTP_PREFIX')

table_lookup ={
    "nyzd.shp": "dcp_zoningdistricts",
    "nyco.shp": "dcp_commercialoverlay",
    "nysp.shp": "dcp_specialpurpose", 
    "nylh.shp": "dcp_limitedheight",
    "nysp_sd.shp": "dcp_specialpurposesubdistricts",
    "nyzma.shp": "dcp_zoningmapamendments"}

def unzip():
    url = ftp_prefix + '/agencysourcedata/dcp/nycgiszoningfeatures_shp.zip'
    file_path = Path(__file__).parent/'nycgiszoningfeatures_shp.zip'
    urllib.request.urlretrieve(url, file_path)
    z = zipfile.ZipFile(file_path)
    z.extractall(Path(__file__).parent/'tmp')
    os.system(f'rm {file_path}')

def shp_to_csv():
    sourcePath = Path(__file__).parent
    shapefiles = [filepath for filepath in Path(sourcePath/'tmp').glob('**/*') if filepath.suffix == '.shp']
    for file in shapefiles:
        srcDS = gdal.OpenEx(str(file))
        gdal.VectorTranslate(
            str(sourcePath.joinpath(f'tmp/{table_lookup[file.name]}.csv')),
            srcDS,
            format='CSV',
            srcSRS='EPSG:2263',
            dstSRS='EPSG:4326',
            options=['-progress'], 
            layerCreationOptions=['GEOMETRY=AS_WKT']
        )

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(table_name):
    base_path = create_base_path(__file__)
    base_path = base_path.replace('dcp_zoningfeatures', table_name)
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    unzip()
    shp_to_csv()
    for table_name in table_lookup.values():
        ETL(table_name)
    clean_up()
