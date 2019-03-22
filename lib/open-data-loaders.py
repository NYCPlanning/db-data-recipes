import os

from urllib.request import urlretrieve
from osgeo import ogr
from osgeo import gdal
from zipfile import ZipFile
from pathlib import Path

def load_from_opendata(url, **options):
  print("Downloading from open data...")
  # Create source directory if not there
  sourcePath = Path.cwd().joinpath('.sources')
  if not sourcePath.exists():
    sourcePath.mkdir()

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