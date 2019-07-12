from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import sys
import csv
csv.field_size_limit(sys.maxsize)

def ETL():
    table_name = 'dpr_parksproperties'
    url = 'https://data.cityofnewyork.us/api/views/xhyz-k7xk/rows.csv?accessType=DOWNLOAD'

    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()