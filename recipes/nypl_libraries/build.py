from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
import json

def ETL(data):
    table_name = 'nypl_libraries'
    base_path = create_base_path(__file__)

    Flow(
        data,
        set_type('lon', type='string'),
        set_type('lat', type='string'),
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":

    url = 'https://refinery.nypl.org/api/nypl/locations/v1.0/locations'
    content = requests.get(url).content
    records = json.loads(content)['locations']
    data = []
    for i in records:
        parsed = dict(
            lon = str(i['geolocation']['coordinates'][0]),
            lat = str(i['geolocation']['coordinates'][1]),
            name = i['name'], 
            zipcode = i['postal_code'], 
            address = i['street_address'],
            locality = i['locality'], 
            region = i['region'],
            open = i['open']
        )
        data.append(parsed)
    ETL(data)