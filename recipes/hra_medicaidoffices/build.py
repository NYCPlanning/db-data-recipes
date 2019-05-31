from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3

def ETL():
    table_name = 'hra_medicaidoffices'
    url = 'https://data.cityofnewyork.us/api/views/fzk8-3ynb/rows.csv?accessType=DOWNLOAD'

    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        add_field('type', 'string', 'Medicaid Office'),
        joined_lower(resources=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))     
    ).process()

if __name__ == '__main__':
    ETL()