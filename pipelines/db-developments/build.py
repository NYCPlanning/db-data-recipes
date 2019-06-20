from dataflows import Flow, load, dump_to_path, printer, set_type
from lib import create_base_path, get_url, dump_2_s3
import csv
import os
import sys

csv.field_size_limit(sys.maxsize)

def Load(recipe, version, **kwargs):
    
    url = get_url(recipe, version)
    print(url)

    return  Flow(
        load(url, name=recipe, format='csv', force_strings=True)
        )

def ETL(): 
    import pprint
    base_path = create_base_path(__file__)
    
    Flow(
        Load('dcp_hny_dob_matching_old', 'latest'),
        Load('dcp_housing_input_dcpattributes', 'latest'),
        Load('dcp_housing_input_lookup_occupancy', 'latest'),
        Load('dcp_housing_input_removals','latest'),
        Load('dcp_housing_input_lookup_status','latest'),
        Load('dob_cofos','latest'),
        # Load('dob_jobapplications','latest'),
        # Load('dob_permitissuance','latest'),
        Load('hpd_hny_units_by_building','latest'),
        Load('hpd_hny_units_by_project', 'latest'),
        dump_2_s3(params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    ETL()