from dataflows import *
from lib import create_base_path, dump_to_s3, rename_field, remove_space

def ETL():
    table_name = 'dob_permitissuance'
    url = 'https://data.cityofnewyork.us/api/views/ipu4-2q9a/rows.csv?accessType=DOWNLOAD'

    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        remove_space(resources=table_name),
        rename_field('SelfCert','Self_Cert'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()