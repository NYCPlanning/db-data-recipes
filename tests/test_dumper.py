def make_client():
    import os 
    import boto3

    session = boto3.session.Session()
    client = session.client('s3',
                            endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    return client

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

def test_dump_to_s3():
    from dataflows import Flow, load, update_resource
    from lib import dump_to_s3, get_resource, create_csv_path, create_s3_path
    from pathlib import Path

    client = make_client()

    bucket='sptkl'
    table_name = 'nycha_policeservice'
    url = 'https://data.cityofnewyork.us/api/views/bvi6-r9nk/rows.csv?accessType=DOWNLOAD'

    base_path = create_csv_path(table_name)

    f = Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path, 
                                                     bucket=bucket))
        )
    f.process()
    objs = client.list_objects_v2(Bucket=bucket, Prefix=table_name)
    contents = objs.get('Contents')
    for i in contents: 
        if i.get('Key') == 'nycha_policeservice/2019-04-01/nycha_policeservice.csv':
            assert i.get('ETag') == '"9bce2fe3b5963c6c2512b2d7a1a3cb97"'
        else:
            pass