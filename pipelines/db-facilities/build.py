from dataflows import Flow, load, dump_to_path
from lib import create_base_path, get_url, dump_to_s3
import csv
import os
import sys

csv.field_size_limit(sys.maxsize)

def Load(recipe, version):
    
    url = get_url(recipe, version)

    return  Flow(
        load(url, name=recipe, format='csv', force_strings=False)
        )

def ETL(): 
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    base_path = create_base_path(__file__)

    Flow(
        Load('doe_universalprek', 'latest'),
        Load('hhc_hospitals', 'latest'),
        Load('dcla_culturalinstitutions','latest'),
        Load('nycha_policeservice','latest'),
        Load('dohmh_daycare','latest'),
        Load('dpr_parksproperties','latest'),
        Load('doe_busroutesgarages','latest'),
        Load('dcp_pops','latest'),
        Load('dcas_colp','latest'),
        Load('dfta_contracts','latest'),
        Load('dycd_afterschoolprograms','latest'),
        Load('bic_tradewaste','latest'),
        Load('dca_operatingbusinesses','latest'), 
        dump_to_s3(params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    ETL()