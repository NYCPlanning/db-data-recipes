from dataflows import Flow, printer, dump_to_sql, checkpoint, load, dump_to_path, set_type
from urllib.request import urlretrieve
from osgeo import ogr
from osgeo import gdal
from zipfile import ZipFile
from pathlib import Path

import os
import csv


# def download_from_nyc_opendata():

def loadFromOpenData():
  print("Download from open data...")
  # Create source directory if not there
  sourcePath = Path.cwd().joinpath('.sources')
  if not sourcePath.exists():
      sourcePath.mkdir()

  url = "https://data.cityofnewyork.us/api/geospatial/ch5p-r223?method=export&format=Original"
  sourceFilePath = sourcePath.joinpath('nyc-landmarks.zip')

  urlretrieve(url, filename=sourceFilePath)

  with ZipFile(sourceFilePath, 'r') as zipObj:
    zipObj.extractall(sourcePath)

  shapefiles = [sourcePath.joinpath(each) for each in os.listdir(sourcePath) if each.endswith('.shp')]
  
  # srcDS = gdal.OpenEx(str(shapefiles[0]))
  # gdal.VectorTranslate(
  #     str(sourcePath.joinpath('nyc-landmarks.csv')),
  #     srcDS,
  #     format='CSV',
  #     dstSRS='EPSG:4326',
  #     options=['-progress'],
  #     layerCreationOptions=['GEOMETRY=AS_WKT']
  # )


# databaseServer = "<IP of database server OR Name of database server"
# databaseName = "<Name of database>"
# databaseUser = "<User name>"
# databasePW = "<User password>"


# connString = "PG: host=%s dbname=%s user=%s password=%s" %(databaseServer,databaseName,databaseUser,databasePW)


def saveAsGeoCSV(path):
  srcDS = gdal.OpenEx(path)
  gdal.VectorTranslate(
      str(sourcePath.joinpath('nyc-landmarks.csv')),
      srcDS,
      format='CSV',
      dstSRS='EPSG:4326',
      layerCreationOptions=['GEOMETRY=AS_WKT']
  )


def saveAsGeojsonColumnCSV(path):
  srcDS = gdal.OpenEx(path)
  gdal.VectorTranslate(
      str(sourcePath.joinpath('nyc-landmarks.csv')),
      srcDS,
      format='CSV',
      SQLDialect='sqlite',
      SQLStatement='select AsGeoJSON(geometry) AS geom, * from IND_Landmark_Points_10_26_18_revised',
      dstSRS='EPSG:4326'
  )


def saveAsGeojson(path):
  srcDS = gdal.OpenEx(path)
  gdal.VectorTranslate(
      str(sourcePath.joinpath('nyc-landmarks.csv')),
      srcDS,
      format='CSV',
      dstSRS='EPSG:4326',
      layerCreationOptions=['GEOMETRY=AS_WKT']
  )

sourcePath = Path.cwd().joinpath('.sources')
shapefiles = [sourcePath.joinpath(each) for each in os.listdir(sourcePath) if each.endswith('.shp')]
shp_path = str(shapefiles[0])


def dump_to_postgis(srcPath):
  dstDS = gdal.OpenEx("PG:host=localhost port=5432 user='pichot' dbname='ceqr_data'", gdal.OF_VECTOR)
  srcDS = gdal.OpenEx(srcPath, open_options=['AUTODETECT_TYPE=YES'])

  gdal.VectorTranslate(
      dstDS,
      srcDS,
      format='PostgreSQL',
      dstSRS='EPSG:4326',
      srcSRS='EPSG:4326',
      layerName='nyc_landmarks.2018',
      accessMode='overwrite'
  )


def remove_nulls(row):
  for k in row:
    row[k] = row[k].replace('<Null>', '')

def print_step(step):
  print(step)

def test_package(package):
  print(package.pkg.descriptor)

Flow(
  # loadFromOpenData(),
  # saveAsGeoCSV(shp_path),

  load('.sources/nyc-landmarks.csv', name='nyc-landmarks'),

  print_step('Removing nulls...'),
  remove_nulls,
  
  dump_to_path(
    out_path='latest'
  ),
  test_package,


  # dump_to_postgis(str(Path.cwd().joinpath('latest/nyc-landmarks.csv')))

).process()

# ogr2ogr -f CSV output.csv source/geo_export_1eb5b445-00a6-4888-a14f-a60a7d9b7beb.shp -progress -lco GEOMETRY=AS_WKT

# ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5432 user='pichot' dbname='ceqr_data'" .sources/geo_export_1c9583f2-18d7-462c-a1cb-0ccf8d7fe430.shp -overwrite -progress -t_srs EPSG:4326 -nln "nyc_landmarks.shp"

# ogr2ogr -oo AUTODETECT_TYPE=YES -s_srs EPSG:4326 -t_srs EPSG:4326 -f "PostgreSQL" PG:"host=localhost port=5432 user='pichot' dbname='ceqr_data'" latest/nyc-landmarks.csv -overwrite -nln "nyc_landmarks.2018"



