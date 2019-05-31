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
    table_name = 'hra_jobcenters'
    base_path = create_base_path(__file__)


    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()


if __name__ == "__main__":
    url = "https://www.google.com/maps/d/kml?mid=1uC3LcicVmGp_CZcyQZkGuf8rLGY&forcekml=1"
    html_content = urllib.request.urlopen(url)
    soup = BeautifulSoup(html_content, 'html.parser')

    descriptions = soup.find_all('description')
    facility = soup.find_all('name')
    data = []
    for d in range(1,len(descriptions)):
        item = descriptions[d].text
        item = item.split('<br>')
        result = {}
        fclty = facility[d+1].text
        for i in range(len(item)):
            result['facility_name'] = fclty.strip()
            result['type'] = 'Job Center'
            parse = item[i].split(': ')
            key = parse[0]
            value = parse[1]
            result[key] = value
        data.append(result)

    ETL(data)