from dataflows.helpers.resource_matcher import ResourceMatcher

def gdal_dump(resources=None):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                file_path = resource['path']
                dumper(file_path)
        yield package.pkg
        
        for res in package:
            yield res

    return func


def dumper(path):
    print(f'dumped to {path}')
#   dstDS = gdal.OpenEx("PG:host=localhost port=5432 user='pichot' dbname='ceqr_data'", gdal.OF_VECTOR)
#   srcDS = gdal.OpenEx(srcPath, open_options=['AUTODETECT_TYPE=YES'])

#   gdal.VectorTranslate(
#       dstDS,
#       srcDS,
#       format='PostgreSQL',
#       dstSRS='EPSG:4326',
#       srcSRS='EPSG:4326',
#       layerName='nyc_landmarks.2018',
#       accessMode='overwrite'
#   )