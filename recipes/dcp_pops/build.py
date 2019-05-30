import zipfile
import requests
import shutil
import io
import os
from pathlib import Path
from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3

# def unzip():
#     url = 'https://nycopendata.socrata.com/api/views/fum3-ejky/files/RmDbuOaF2Ix4qRBCywjyAQoeRy55Hz8L2kQHZy8R3Mc?filename=DCP_POPS.zip'
#     r = requests.get(url)
#     z = zipfile.ZipFile(io.BytesIO(r.content))
#     z.extractall(Path(__file__).parent/'tmp')

# def rm_tmp():
#     os.system(f'rm -r {str(Path(__file__).parent)}/tmp')

# def mdb_to_csv():
#     os.system(f'sh {str(Path(__file__).parent)}/mdb_to_csv.sh')

def ETL():

    table_name = 'dcp_pops'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'dcp_pops.csv'
    Flow(
        # load(f'{str(Path(__file__).parent)}/tmp/dcp_pops.csv', name=table_name, format='csv', force_strings=True),
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),  
        # set_type('dcp_record', resources=table_name, type='string'),
        # set_type('community_district', resources=table_name, type='string'),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
        ).process()

if __name__ == '__main__':
#     unzip()
#     mdb_to_csv()
    ETL()
#     rm_tmp()

