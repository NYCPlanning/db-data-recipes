from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3

def ETL():
    table_name = 'nysdoh_healthfacilities'
    url = 'https://health.data.ny.gov/api/views/vn5v-hh5r/rows.csv?accessType=DOWNLOAD'

    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()