from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
import json

def ETL(data):
    table_name = 'fbop_corrections'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        delete_fields(['special','hascamp','hasfsl','hasfdc','hassff','hasihp']),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":

    sites = {
        'Brooklyn MDC': 'https://www.bop.gov/PublicInfo/execute/phyloc?todo=query&output=json&code=bro',
        'New York MCC': 'https://www.bop.gov/PublicInfo/execute/phyloc?todo=query&output=json&code=nym',
        'New York RRM': 'https://www.bop.gov/PublicInfo/execute/phyloc?todo=query&output=json&code=cnk'
    }
    def get_location(facility):
        response = requests.get(sites.get(facility)).content
        location = json.loads(response)
        return location.get('Locations')[0]
    
    data = []
    data.append(get_location('Brooklyn MDC'))
    data.append(get_location('New York MCC'))
    data.append(get_location('New York RRM'))

    ETL(data)