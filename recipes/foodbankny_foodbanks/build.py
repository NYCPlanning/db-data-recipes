# from dataflows import *
# from lib import joined_lower, create_base_path
# from lib import dump_to_s3
# import requests
# from bs4 import BeautifulSoup
# from urllib.request import Request, urlopen
# import ssl
# import json

# def ETL(data):
#     table_name = 'foodbankny_foodbanks'
#     base_path = create_base_path(__file__)

#     Flow(
#         data,
#         update_resource(None, name=table_name),
#         update_resource(resources=table_name, path=table_name+'.csv'),
#         joined_lower(resources=table_name),
#         dump_to_s3(resources=table_name, params=dict(base_path=base_path))
#     ).process()

# if __name__ == "__main__":

#     site= 'https://www.foodbanknyc.org/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=83cc04ac0d&load_all=0&layout=1&lat=40.7983474111969&lng=-73.9395518&nw%5B%5D=41.15908281222903&nw%5B%5D=-74.4614023859375&se%5B%5D=40.43761201016478&se%5B%5D=-73.4177012140625'
#     hdr = {'User-Agent': 'Mozilla/5.0'}
#     req = Request(site,headers=hdr)
#     gcontext = ssl.SSLContext()
#     page = urlopen(req, context=gcontext)
#     soup = BeautifulSoup(page, features="lxml")
#     p = soup.find('p').get_text()
#     data = json.loads(p)

#     ETL(data)

from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import ssl
import json
from pathlib import Path
import pandas as pd

def ETL(data):
    table_name = 'foodbankny_foodbanks'
    base_path = create_base_path(__file__)
    Flow(
        # data,
        load(f'{str(Path(__file__).parent)}/foodbankny_foodbanks.csv'),
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":

    site= 'https://www.foodbanknyc.org/wp-admin/admin-ajax.php?action=asl_load_stores&nonce=83cc04ac0d&load_all=0&layout=1&lat=40.7983474111969&lng=-73.9395518&nw%5B%5D=41.15908281222903&nw%5B%5D=-74.4614023859375&se%5B%5D=40.43761201016478&se%5B%5D=-73.4177012140625'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(site,headers=hdr)
    gcontext = ssl.SSLContext()
    page = urlopen(req, context=gcontext)
    soup = BeautifulSoup(page, features="lxml")
    p = soup.find('p').get_text()
    data = json.loads(p)
    pd.DataFrame(data).to_csv(f'{str(Path(__file__).parent)}/foodbankny_foodbanks.csv')
    ETL(data)