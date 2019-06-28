from dataflows import Flow, load
from lib import dump_to_s3, get_resource, create_base_path, joined_lower
import os

def ETL():
    table_name = 'dob_cofos'
    url = 'https://raw.githubusercontent.com/NYCPlanning/db-developments/master/developments_build/data/dob_cofos.csv'
    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()