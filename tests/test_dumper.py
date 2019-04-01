def test_dump_to_postgis():
    from dataflows import Flow, load
    from lib import dump_to_postgis
    from sqlalchemy import create_engine
    import os 
    import sys
    
    table_name = 'nycha_policeservice'
    url = 'https://data.cityofnewyork.us/api/views/bvi6-r9nk/rows.csv?accessType=DOWNLOAD'
    f = Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        dump_to_postgis(resources=table_name, db_table_name=table_name, engine='env://DATAFLOWS_DB_ENGINE')
        )
    
    f.process()

    engine = create_engine(os.environ.get('DATAFLOWS_DB_ENGINE'))
    result = list(dict(x) for x in engine.execute(f'select wkb_geometry from {table_name} limit 1'))
    assert result == [{'wkb_geometry': '0101000020E61000001FCF149C617F52C0D7CE5B5AD9494440'}]

# def test_dump_to_s3():
#     from dataflows import Flow, load
#     from lib import dump_to_s3, get_resource
#     import os 
    
#     table_name = 'nycha_policeservice'
#     url = 'https://data.cityofnewyork.us/api/views/bvi6-r9nk/rows.csv?accessType=DOWNLOAD'

#     f = Flow(
#         load(url, name=table_name, format='csv', force_strings=False),
#         dump_to_s3(resources=table_name, base_path='/test/', bucket='sptkl')
#         )

#     f.process()