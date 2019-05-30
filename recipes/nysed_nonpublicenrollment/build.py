from dataflows import Flow, load, dump_to_path, update_resource
from lib import dump_to_s3, get_resource, create_base_path, joined_lower
import os

def ETL():
    table_name = 'nysed_nonpublicenrollment'
    url = 'http://www.p12.nysed.gov/irs/statistics/nonpublic/2018-19_NonPub_EnrollmentbyGrade.xlsx'
    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='xlsx', force_strings=True),
        joined_lower(resources=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()