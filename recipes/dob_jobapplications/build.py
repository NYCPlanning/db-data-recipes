from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request

url = "https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD"

def download_file():
    os.mkdir(Path(__file__).parent/'tmp')
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    urllib.request.urlretrieve(url, file_path)
    os.system(f"sed -i '1s/.*/'{target_header}'/' {file_path}")

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(table_name):
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'dob_jobapplications'
    target_header = "jobnumber,jobdocnumber,borough,housenumber,streetname,block,lot,bin,jobtype,jobstatus,jobstatusdesc,\
        latestactiondate,buildingtype,communityboard,cluster,landmarked,adultestab,loftboard,cityowned,littlee,pcfiled,\
        efilingfiled,plumbing,mechanical,boiler,fuelburning,fuelstorage,standpipe,sprinkler,firealarm,equipment,firesuppression,\
        curbcut,other,otherdesc,applicantfirstname,applicantlastname,applicantprofessionaltitle,applicantlicensenumber,\
        professionalcert,prefilingdate,paid,fullypaid,assigned,approved,fullypermitted,initialcost,totalestfee,feestatus,\
        existingzoningsqft,proposedzoningsqft,horizontalenlrgmt,verticalenlrgmt,enlargementsqfootage,streetfrontage,\
        existingnumstories,proposednumstories,existingheight,proposedheight,existingdwellingunits,proposeddwellingunits,\
        existingoccupancy,proposedoccupancy,sitefill,zoningdist1,zoningdist2,zoningdist3,specialdistrict1,specialdistrict2,\
        ownertype,nonprofit,ownerfirstname,ownerlastname,ownerbusinessname,ownerhousenumber,ownerhousestreetname,city,state,\
        zip,ownerphone,jobdescription,dobrundate,jobs1no,totalconstructionfloorarea,withdrawalflag,signoffdate,specialactionstatus,\
        specialactiondate,buildingclass,jobnogoodcount,latitude,longitude,councildistrict,censustract,nta,gisbin"
    download_file()
    ETL(table_name)
    clean_up()