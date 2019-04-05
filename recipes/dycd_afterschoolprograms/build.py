from dataflows import *
from lib import joined_lower, create_csv_path
import os
from lib import dump_to_s3

def ETL():
    table_name = 'dycd_afterschoolprograms'
    url='https://data.cityofnewyork.us/api/views/mbd7-jfnc/rows.csv?accessType=DOWNLOAD'

    base_path = create_csv_path(table_name)

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()