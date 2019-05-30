from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3, rename_field
from pathlib import Path

def ETL():
    table_name = 'nysed_activeinstitutions'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'nysed_activeinstitutions.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        rename_field('gis_longitute_(x)', 'gis_longitute_x'), 
        rename_field('gis_latitude_(y)', 'gis_latitude_y'),
        rename_field('federal_information_processing_standard_(fips)_state_code', 
                    'federal_information_processing_standard_fips_state_code'), 
        rename_field('federal_information_processing_standard_(fips)_county_code', 
                    'federal_information_processing_standard_fips_county_code'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()