def test_dump_to_postgis():
    from dataflows import Flow, printer, load, dump_to_path, update_resource
    from lib import dump_to_postgis, create_csv_path, dump_to_s3, get_resource
    from lib.rename_field import rename_field
    from sqlalchemy import create_engine
    import os 
    import sys
    import csv
    csv.field_size_limit(sys.maxsize)

    table_name = 'dpr_parksproperties'
    url = 'https://data.cityofnewyork.us/api/views/qyck-9ptq/rows.csv?accessType=DOWNLOAD'
    path = create_csv_path(__file__)
    f = Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        dump_to_path(path),
        # dump_to_s3(resources=table_name,
        #            params=dict(bucket='sptkl', base_path=path)),
        update_resource(resources=table_name, path= f'{path}/{table_name}.csv'),
        # get_resource(resources=table_name, key='path'),
        # get_resource(resources=table_name, key='path'),
        dump_to_postgis(resources=table_name, 
                        engine='postgres://postgres@localhost:5432/postgres',
                        drcSRS='EPSG:4326',
                        srcSRS='EPSG:4326',
                        ), 
        
        )
    
    f.process()

if __name__ == "__main__":
    test_dump_to_postgis()