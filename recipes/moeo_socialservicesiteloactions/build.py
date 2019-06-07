from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request

ftp_prefix = os.environ.get('FTP_PREFIX')
file_name = 'Social_Service_Site_Location_DCP_052319.xlsx'

def download_file():
    url = ftp_prefix + '/agencysourcedata/moeo/' + file_name
    file_path = Path(__file__).parent/f'{file_name}'
    urllib.request.urlretrieve(url, file_path)

def clean_up(): 
    path = Path(__file__).parent/f'{file_name}'
    os.system(f'rm -r {path}')

def ETL():
    table_name = 'moeo_socialservicesiteloactions'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/f'{file_name}'
    Flow(
        load(str(file_path), name=table_name, force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    download_file()
    ETL()
    clean_up()