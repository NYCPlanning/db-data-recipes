from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import ssl
import json

def ETL(data):
    table_name = 'nysdoccs_corrections'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":

    site= 'http://www.doccs.ny.gov/faclist.html'
    soup = BeautifulSoup(requests.get(site).content, features='html.parser')
    data = []
    for i in soup.find_all('tr'):
        info = [item.strip() for item in i.get_text().split('\n') if item not in ['', 'Map', 'Driving Directions']]
        result = dict(
            facility_name = info[0],
            address = ', '.join(info[1:-2]).split(', (')[0],
            security_level = info[-2],
            male_or_female = info[-1]
        )
        data.append(result)
    ETL(data)