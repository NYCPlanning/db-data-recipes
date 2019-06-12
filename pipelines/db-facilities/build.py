from dataflows import Flow, load, dump_to_path, printer, set_type
from lib import create_base_path, get_url, dump_2_s3
import csv
import os
import sys

csv.field_size_limit(sys.maxsize)

def Load(recipe, version, **kwargs):
    
    url = get_url(recipe, version)
    print(url)
    try: 
        return  Flow(
            load(url, name=recipe, format='csv')
            )
    except: 
        print(recipe)

def ETL(): 
    import pprint
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
        Load('doitt_libraries','latest'),
        Load('sbs_workforce1','latest'),
        Load('nysdec_solidwaste','latest'),
        Load('nysomh_mentalhealth','latest'),
        Load('nysdoh_healthfacilities','latest'),
        Load('nysopwdd_providers','latest'),
        Load('usnps_parks','latest'),
        Load('dca_operatingbusinesses', 'latest'),
        Load('dep_wwtc', 'latest'),
        Load('foodbankny_foodbanks', 'latest'),
        Load('bpl_libraries', 'latest'),
        Load('qpl_libraries', 'latest'),
        Load('dsny_mtsgaragemaintenance', 'latest'),
        Load('doe_lcgms', 'latest'),
        Load('nysdoh_nursinghomes', 'latest'),
        Load('nysed_activeinstitutions', 'latest'),
        Load('usdot_airports', 'latest'),
        Load('nysed_nonpublicenrollment', 'latest'),
        Load('nypl_libraries', 'latest'),
        Load('usdot_ports', 'latest'),
        Load('dot_mannedfacilities', 'latest'),
        Load('dot_bridgehouses', 'latest'),
        Load('dot_ferryterminals', 'latest'),
        Load('dot_publicparking', 'latest'),
        Load('hra_centers', 'latest'),
        Load('nysdec_lands', 'latest'),
        Load('nycha_communitycenters', 'latest'),
        Load('nysdec_lands', 'latest'),
        Load('nycha_communitycenters', 'latest'),
        Load('moeo_socialservicesiteloactions', 'latest'),
        Load('fbop_corrections', 'latest'),
        Load('nysparks_historicplaces', 'latest'),
        Load('uscourts_courts', 'latest'),
        Load('nysocfs_offices', 'latest'),
        Load('nysoasas_programs', 'latest'),
        Load('nysdoccs_corrections', 'latest'),
        Load('nycdoc_corrections', 'latest'),
        Load('dot_pedplazas', 'latest'),
        Load('nycourts_courts', 'latest')
        dump_2_s3(params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    ETL()