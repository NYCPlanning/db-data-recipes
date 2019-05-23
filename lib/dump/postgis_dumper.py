import os
import logging
import mimetypes
from lib.parse_engine import parse_engine
from osgeo import ogr
from osgeo import gdal
from pathlib import Path
import tempfile
from dataflows.processors.dumpers.file_dumper import FileDumper

class PostgisDumper(FileDumper):
    def __init__(self,
                 resource=None,
                 db_table_name=None,
                 srcSRS='EPSG:4326', 
                 dstSRS='EPSG:4326',
                 engine='env://DATAFLOWS_DB_ENGINE',
                 **options):

        super(PostgisDumper, self).__init__(options)
        if engine.startswith('env://'):
            env_var = engine[6:]
            engine = os.environ.get(env_var)
            if engine is None:
                raise ValueError("Couldn't connect to DB - "
                                 "Please set your '%s' environment variable" % env_var)

        self.engine = parse_engine(engine)
        self.db_table_name = db_table_name
        self.srcSRS = srcSRS
        self.dstSRS = dstSRS
        
    def process_resource(self, resource):
        if resource.res.name in self.file_formatters:
            schema = resource.res.schema

            temp_file = tempfile.NamedTemporaryFile(mode="w+", suffix='.csv', delete=False, newline='')
            writer_kwargs = {'use_titles': True} if self.use_titles else {}
            writer = self.file_formatters[resource.res.name](temp_file, schema, **writer_kwargs)

            return self.rows_processor(resource,
                                       writer,
                                       temp_file)
        else:
            return resource

    def write_file_to_output(self, filename, path):
        if Path(path).suffix == '.csv':
            table_name = Path(path).stem if self.db_table_name is None else self.db_table_name
            PostgisDumper.put_object(table_name,
                                    self.srcSRS, self.dstSRS,
                                    filename, self.engine)
        else: #ignore datapacakge.json when dumping to postgis
            pass

    @staticmethod
    def put_object(db_table_name, srcSRS, dstSRS, filename, engine):
        dstDS = gdal.OpenEx(engine, gdal.OF_VECTOR)
        srcDS = gdal.OpenEx(filename, open_options=['AUTODETECT_TYPE=NO', 'GEOM_POSSIBLE_NAMES=the_geom'])

        gdal.VectorTranslate(
            dstDS,
            srcDS,
            format='PostgreSQL',
            dstSRS=dstSRS,
            srcSRS=srcSRS,
            layerName=db_table_name,
            accessMode='overwrite')