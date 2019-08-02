from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import os
from pathlib import Path
import urllib.request
import pandas as pd

ftp_prefix = os.environ.get('FTP_PREFIX')

def download_file():
    url = ftp_prefix + '/agencysourcedata/moeo/Social_Service_Site_Location_DCP_052319.xlsx'
    os.mkdir(Path(__file__).parent/'tmp')
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.xlsx'
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
    table_name = 'moeo_socialservicesiteloactions'
    download_file()
    file_path = Path(__file__).parent/'tmp'/f'{table_name}.xlsx'
    classification_link = 'https://raw.githubusercontent.com/NYCPlanning/db-data-recipes/master/recipes/moeo_socialservicesiteloactions/moeo_socialservicesiteloactions_classification.csv'
    df1 = pd.ExcelFile(file_path).parse('Sheet1', dtype = 'str')
    df2 = pd.read_csv(classification_link, dtype = 'str')
    df = pd.merge(df1, df2, how = 'left', on = 'PROGRAM_NAME')
    df.fillna('',inplace=True)
    data = df.to_dict('records')
    ETL(data)
    clean_up()