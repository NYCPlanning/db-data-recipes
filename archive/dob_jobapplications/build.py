from lib import joined_lower, create_base_path, dump_to_s3
from lib.s3.make_client import make_client
from boto3.s3.transfer import TransferConfig
import os
from pathlib import Path
import urllib.request
import os
import mimetypes
import time

def download(table_name):
    beg_ts = time.time()
    url = 'https://data.cityofnewyork.us/api/views/ic3t-wcy2/rows.csv?accessType=DOWNLOAD'
    os.mkdir(Path(__file__).parent/'tmp')
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    urllib.request.urlretrieve(url, file_path)
    end_ts = time.time()
    print(f'Downloaded to {file_path}, elapsed time: {end_ts - beg_ts}')


def replace_header(table_name):
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    cmd = f"sed -i \"1s/.*/jobnumber,jobdocnumber,borough,housenumber,streetname,block,lot,bin,jobtype,jobstatus,jobstatusdesc,latestactiondate,buildingtype,communityboard,cluster,landmarked,adultestab,loftboard,cityowned,littlee,pcfiled,efilingfiled,plumbing,mechanical,boiler,fuelburning,fuelstorage,standpipe,sprinkler,firealarm,equipment,firesuppression,curbcut,other,otherdesc,applicantfirstname,applicantlastname,applicantprofessionaltitle,applicantlicensenumber,professionalcert,prefilingdate,paid,fullypaid,assigned,approved,fullypermitted,initialcost,totalestfee,feestatus,existingzoningsqft,proposedzoningsqft,horizontalenlrgmt,verticalenlrgmt,enlargementsqfootage,streetfrontage,existingnumstories,proposednumstories,existingheight,proposedheight,existingdwellingunits,proposeddwellingunits,existingoccupancy,proposedoccupancy,sitefill,zoningdist1,zoningdist2,zoningdist3,specialdistrict1,specialdistrict2,ownertype,nonprofit,ownerfirstname,ownerlastname,ownerbusinessname,ownerhousenumber,ownerhousestreetname,city,state,zip,ownerphone,jobdescription,dobrundate,jobs1no,totalconstructionfloorarea,withdrawalflag,signoffdate,specialactionstatus,specialactiondate,buildingclass,jobnogoodcount,latitude,longitude,councildistrict,censustract,nta,gisbin/\" {file_path}"
    os.system(cmd)

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(table_name):
    key = str(Path(create_base_path(__file__))/f'{table_name}.csv')
    file_path = str(Path(__file__).parent/'tmp'/f'{table_name}.csv')
    content_type, _ = mimetypes.guess_type(key)
    client = make_client()
    bucket = os.environ.get('BUCKET')
    config = TransferConfig(multipart_threshold=1024^2*100, max_concurrency=10,
                        multipart_chunksize=1024^2*100, use_threads=True)

    beg_ts = time.time()
    client.upload_file(
                Filename=file_path,
                Bucket=bucket,
                Config = config,
                ExtraArgs={ 'ACL': 'public-read', 'ContentType': content_type or 'text/plain'},
                Key=key)
    end_ts = time.time()
    print(f'dumped to {key}, elapsed time: {end_ts - beg_ts}')

if __name__ == '__main__':
    table_name = 'dob_jobapplications'
    download(table_name)
    replace_header(table_name)
    ETL(table_name)
    clean_up()