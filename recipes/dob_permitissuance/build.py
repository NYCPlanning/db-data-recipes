from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request

url = "https://data.cityofnewyork.us/api/views/ipu4-2q9a/rows.csv?accessType=DOWNLOAD"

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
    table_name = 'dob_permitissuance'
    target_header = "BOROUGH,Binnum,Housenum,StreetName,Jobnum,Jobdocnum,JobType,Self_Cert,Block,Lot,CommunityBoard,\
        ZipCode,BldgType,Residential,SpecialDistrict1,SpecialDistrict2,WorkType,PermitStatus,FilingStatus,PermitType,\
        PermitSequencenum,PermitSubtype,OilGas,SiteFill,FilingDate,IssuanceDate,ExpirationDate,JobStartDate,PermitteeFirstName,\
        PermitteeLastName,PermitteeBusinessName,PermitteePhonenum,PermitteeLicenseType,PermitteeLicensenum,ActSuperintendent,\
        PermitteeOtherTitle,HICLicense,SiteSafetyMgrFirstName,SiteSafetyMgrLastName,SiteSafetyMgrBusinessName,\
        SuperintendentFirstLastName,SuperintendentBusinessName,OwnerBusinessType,NonProfit,OwnerBusinessName,OwnerFirstName,\
        OwnerLastName,OwnerHousenum,OwnerHouseStreetName,OwnerHouseCity,OwnerHouseState,OwnerHouseZipCode,OwnerPhonenum,\
        DOBRunDate,PERMITSINO,LATITUDE,LONGITUDE,COUNCILDISTRICT,CENSUSTRACT,NTANAME"
    download_file()
    ETL(table_name)
    clean_up()