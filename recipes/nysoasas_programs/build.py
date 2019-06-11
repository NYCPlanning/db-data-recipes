from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from urllib.request import Request, urlopen
import ssl
import json
import pandas as pd
import io

def ETL(data):
    table_name = 'nysoasas_programs'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    url = 'https://www.oasas.ny.gov/providerDirectory/download/Treatment_Providers_OASAS_Directory_Search_11-Jun-19.csv'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url,headers=hdr)
    gcontext = ssl.SSLContext()
    page = urlopen(req, context=gcontext).read()
    data = pd.read_csv(io.StringIO(page.decode('utf-8'))).to_dict('records')
    ETL(data)