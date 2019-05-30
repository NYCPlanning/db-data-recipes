from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
from pathlib import Path

def ETL():
    table_name = 'bic_tradewaste'
    url = 'https://data.cityofnewyork.us/api/views/hsjb-p5ky/rows.csv'
    base_path = create_base_path(__file__)
    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()