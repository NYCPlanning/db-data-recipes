from dataflows import *
from lib import create_base_path, dump_to_s3, remove_space_lower, rename_field

def ETL():
    table_name = 'dob_jobapplications'
    url = 'https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD'

    base_path = create_base_path(__file__)

    Flow(
        load(url, name=table_name, format='csv', force_strings=True),
        remove_space_lower(resources=table_name),
        rename_field('binnumber', 'bin'),
        rename_field('jobstatusdescrp', 'jobstatusdesc'),
        rename_field('otherdescription','otherdesc'),
        rename_field('existingnoofstories','existingnumstories'),
        rename_field('proposednoofstories','proposednumstories'),
        rename_field('ownerphonenumber', 'ownerphone'),
        rename_field('gislatitude', 'latitude'),
        rename_field('gislongitude', 'longitude'),
        rename_field('giscouncildistrict', 'councildistrict'),
        rename_field('giscensustract', 'censustract'),
        rename_field('gisntaname', 'nta'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    ETL()