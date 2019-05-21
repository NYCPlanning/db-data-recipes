from dataflows import Flow, load, update_resource
from lib import dump_to_s3, get_resource, create_base_path, joined_lower
import os

def ETL():
    table_name = 'nysdoh_nursinghomes'
    url = 'https://health.data.ny.gov/api/views/izta-vnpq/rows.csv?accessType=DOWNLOAD'
    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()