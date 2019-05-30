from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import urllib.request
import ssl
import json

def ETL(data):
    table_name = 'hra_snapcenters'
    base_path = create_base_path(__file__)


    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        # dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()


if __name__ == "__main__":
    url = r"https://www1.nyc.gov/site/hra/locations/snap-locations.page"
    html_content = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_content, 'html.parser')
    data = []
    for i in range(len(soup.find_all("b"))):
        fclty = soup.find_all("b")[i].text.replace(':', '')
        addr = soup.find_all("b")[i].next_sibling.replace(':', '').replace('\xa0', '')[1:]
        address = [x.strip() for x in addr[:-5].split(',')]
        address.append(addr[-5:])
        result = {}
        result['facility_name'] = fclty
        result['address'] = address[0]
        result['address2'] = address[1]
        result['city'] = address[2]
        result['state'] = address[3]
        result['zipcode'] = address[4]
        result['type'] = 'SNAP Center'
        data.append(result)
        
    ETL(data)
