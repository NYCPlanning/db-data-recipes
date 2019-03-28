def test_dump_to_postgis():
    from dataflows import Flow, printer, load, dump_to_path, update_resource
    from lib import dump_to_postgis
    from lib.rename_field import rename_field
    from sqlalchemy import create_engine
    import os 
    import datetime
    import sys
    import csv
    csv.field_size_limit(sys.maxsize)

    table_name = 'dpr_parksproperties'
    url = 'https://data.cityofnewyork.us/api/views/qyck-9ptq/rows.csv?accessType=DOWNLOAD'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())
    f = Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        dump_to_path(path),
        update_resource(resources=table_name, path=os.path.join('tests/2019-03-28', f'{table_name}.csv')),
        dump_to_postgis(resources=table_name, 
                        engine='postgresql://postgres:0312@localhost:5432/postgres',
                        drcSRS='EPSG:4326',
                        srcSRS='EPSG:4326',
                        )
        )
    f.process()

if __name__ == "__main__":
    test_dump_to_postgis()
    # Check validity
    # engine = create_engine('sqlite:///test.db')
    # result = list(dict(x) for x in engine.execute('select * from output_table'))
    # assert result == data
