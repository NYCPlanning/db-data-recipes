from dataflows.helpers.resource_matcher import ResourceMatcher
from lib.parse_engine import parse_engine
from osgeo import ogr
from osgeo import gdal

def postgis_dumper(resources=None, **options):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                name = resource['name']
                file_path = resource['path']
                dump_helper(name, file_path, options)
        yield package.pkg
        
        for res in package:
            yield res

    return func


def dump_helper(name, path, options):
    engine = parse_engine(options.get('engine', ''))
    host = engine.get('host')
    port = engine.get('port')
    user = engine.get('user')
    dbname = engine.get('database')

    dstDS = gdal.OpenEx(f'PG:host={host} port={port} user={user} dbname={dbname}', gdal.OF_VECTOR)
    srcDS = gdal.OpenEx(path, open_options=['AUTODETECT_TYPE=YES', 'GEOM_POSSIBLE_NAMES=the_geom'])
    DstSRS = options.get('drcSRS')
    SrcSRS = options.get('srcSRS')

    gdal.VectorTranslate(
        dstDS,
        srcDS,
        format='PostgreSQL',
        dstSRS=DstSRS,
        srcSRS=SrcSRS,
        layerName=name,
        accessMode='overwrite')