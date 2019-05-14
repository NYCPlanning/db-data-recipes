from dataflows import *
from lib import joined_lower, create_base_path, dump_to_s3
import json
import requests

def ETL(data):
    table_name = 'bpl_libraries'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == '__main__':
    url='https://www.bklynlibrary.org/locations/json'
    response = requests.get(url)
    content = json.loads(response.content)
    
    data = []
    for i in content['locations']:
        data.append(i['data'])
    
    ETL(data)