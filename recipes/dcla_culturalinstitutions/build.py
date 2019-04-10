from dataflows import Flow, load
from lib import dump_to_s3, get_resource, create_base_path, joined_lower
import os

def ETL():
    table_name = 'dcla_culturalinstitutions'
    url = 'https://data.cityofnewyork.us/api/views/u35m-9t32/rows.csv?accessType=DOWNLOAD'
    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()