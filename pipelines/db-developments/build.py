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
        load(url, name=recipe)
        )

def ETL(): 
    import pprint
    base_path = create_base_path(__file__)
    
    Flow(
        Load('housing_input_hny_job_manual', 'latest'),
        # Load('housing_input_dcpattributes', 'latest'),
        # Load('housing_input_lookup_occupancy', 'latest'),
        Load('housing_input_removals','latest'),
        # Load('housing_input_lookup_status','latest'),
        # Load('dob_cofos','latest'),
        # Load('dob_jobapplications','latest'),
        # Load('dob_permitissuance','latest'),
        Load('hpd_hny_units_by_building','latest'),
        Load('hpd_hny_units_by_project', 'latest'),
        dump_2_s3(params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    ETL()