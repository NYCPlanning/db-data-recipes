from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3, get_hnum, get_sname, get_zipcode
import requests
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import ssl
import json
import pandas as pd
import io

def ETL(data):
    table_name = 'nycdoc_corrections'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    url = 'https://www1.nyc.gov/site/doc/about/facilities-locations.page'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(url,headers=hdr)
    gcontext = ssl.SSLContext()
    page = urlopen(req, context=gcontext)
    soup = BeautifulSoup(page, features='html.parser')
    data = []
    targets = ['Bellevue Hospital Prison Ward (BHPW)', 
            'Brooklyn Detention Complex (BKDC)', 
            'Elmhurst Hospital Prison Ward (EHPW)', 
            'Manhattan Detention Complex (MDC)', 
            'Queens Detention Complex (QDC)', 
            'Vernon C. Bain Center (VCBC)']
    for i in soup.find_all('p'):
        info = i.get_text('|').split('|')
        if info[0] in targets:
            result = dict(
                name = info[0], 
                address1 = info[1], 
                address2 = info[2],
                house_number = get_hnum(info[1]), 
                street_name = get_sname(info[1]),
                zipcode = get_zipcode(info[2])
            )
            data.append(result)
    ETL(data)