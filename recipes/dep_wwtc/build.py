from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import ssl
import json

def ETL(data):
    table_name = 'dep_wwtc'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        add_computed_field([dict(target=dict(name = 'population_served', type = 'integer'),
                            operation=lambda row: int(row['Population Served'].replace(',', ''))
                            )
                        ]),
        delete_fields(fields=['Population Served']),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    site = 'https://www1.nyc.gov/html/dep/html/wastewater/wwsystem-plants.shtml'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = Request(site,headers=hdr)
    gcontext = ssl.SSLContext()
    page = urlopen(req, context=gcontext)
    soup = BeautifulSoup(page, features="lxml")
    data = []
    for i in soup.find_all('table', class_='table table-bordered'):
        i1 = i.find('tbody')
        title = i1.find('h2').get_text()
        result = {}
        for a in i1.find_all('tr'):
            try: 
                key = a.find('th').get_text().replace(':', '')
                val = a.find('td').get_text().strip()
                result[key] = val
            except: 
                continue
        data.append(result)

    ETL(data)