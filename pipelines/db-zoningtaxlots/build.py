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
        Load('dcp_commercialoverlay', 'latest'),
        Load('dcp_limitedheight', 'latest'),
        Load('dcp_mih','latest'),
        Load('dof_dtm','latest'),
        Load('dcp_specialpurpose','latest'),
        Load('dcp_specialpurposesubdistricts','latest'),
        Load('dcp_zoningdistricts','latest'),
        Load('dcp_zoningmapamendments','latest'),
        Load('dcp_zoningtaxlots', 'latest'),
        dump_2_s3(params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    ETL()