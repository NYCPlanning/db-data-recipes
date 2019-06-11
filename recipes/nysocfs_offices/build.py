from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import ssl
import json

def ETL(data):
    table_name = 'nysocfs_offices'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    url = 'https://ocfs.ny.gov/main/regionaloffices_main.asp'
    response = requests.get(url)\
                .content\
                .decode("utf-8")\
                .split('<h2>New York City Regional Office</h2>')[1]\
                .split('<h2>Rochester Regional Office</h2>')[0].strip()
    data = []
    for i in response.split('<h3>'):
        if i == '': 
            continue
        else: 
            facility_type = i.split('</h3>')[0]
            soup = BeautifulSoup(i.split('</h3>')[-1].split('Phone')[0], 'html.parser')
            parsed = soup.get_text().strip().split('\n')
            result = dict(
                facility_type = facility_type.strip().replace('&ndash;', '-'), 
                manager = parsed[0].strip(), 
                address = ', '.join(parsed[1:])\
                            .replace('\r', '')\
                            .split('(')[0],
            )
            data.append(result)
    ETL(data)