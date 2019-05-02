from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
from pathlib import Path

def ETL():
    table_name = 'doe_lcgms'
    base_path = create_base_path(__file__)
    Flow(
        load('LCGMS_SchoolData_20190502_1024.xls', name=table_name, sheet=1, force_strings=True),
        joined_lower(resources=table_name),
        printer(),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    # download_file()
    ETL()