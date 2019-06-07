from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request
import pandas as pd
import re

input_name = 'pka.csv'
output_name = 'output.csv'

def download_file():
    url = 'https://maps.nyc.gov/prek/data/pka/pka.csv'
    file_path = Path(__file__).parent/f'{input_name}'
    urllib.request.urlretrieve(url, file_path)

def clean_up(): 
    input_path = Path(__file__).parent/f'{input_name}'
    output_path = Path(__file__).parent/f'{output_name}'
    os.system(f'rm -r {input_path}')
    os.system(f'rm -r {output_path}')

def ETL():
    table_name = 'doe_universalprek'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/f'{input_name}'
    df = pd.read_csv(file_path, encoding = 'unicode_escape')
    output_path = Path(__file__).parent/f'{output_name}'
    df['NOTE'] = df.NOTE.astype(str).apply(lambda x: re.sub(r'[,\<b>\</b>]','',x))
    df.to_csv(Path(__file__).parent/f'{output_name}')
    Flow(
        load(str(output_path), name=table_name, force_strings=True),
        joined_lower(resources=table_name), 
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    download_file()
    ETL()
    clean_up()