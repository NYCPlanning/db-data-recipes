from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request
import pandas as pd
import re

def download_file():
    url = 'https://data.cityofnewyork.us/api/views/3bkj-34v2/files/56813139-9b9d-44fb-b81d-068553b7a9b7?download=true&filename=LCGMS_SchoolData(additional%20geocoded%20fields%20added).csv'
    os.mkdir(Path(__file__).parent/'tmp')
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    urllib.request.urlretrieve(url, file_path)

def clean_up():
    tmp_path = Path(__file__).parent/'tmp'
    os.system(f'rm -r {tmp_path}')

def ETL(data):
    
    base_path = create_base_path(__file__)
    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    table_name = 'doe_lcgms'
    download_file()
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.csv'
    df = pd.read_csv(file_path, encoding = 'unicode_escape', dtype = 'str')
    df.fillna('',inplace=True)
    df = df.to_dict('records')
    ETL(df)
    clean_up()