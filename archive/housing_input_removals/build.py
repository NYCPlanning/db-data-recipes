from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3

def ETL():
    table_name = 'housing_input_removals'
    url = 'https://raw.githubusercontent.com/NYCPlanning/db-developments/master/developments_build/data/housing_input_removals.csv'
    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        add_field('b', 'string', ''),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()