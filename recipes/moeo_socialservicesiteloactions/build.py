from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request

ftp_prefix = os.environ.get('FTP_PREFIX')

def download_file():
    url = ftp_prefix + '/agencysourcedata/moeo/Social_Service_Site_Location_DCP_052319.xlsx'
    os.mkdir(Path(__file__).parent/'tmp')
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.xlsx'
    urllib.request.urlretrieve(url, file_path)

def clean_up(): 
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL():
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.xlsx'
    Flow(
        load(str(file_path), name=table_name, format='xlsx', sheet=1, force_strings=True),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'moeo_socialservicesiteloactions'
    download_file()
    ETL()
    clean_up()