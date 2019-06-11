from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
import json

def ETL(data):
    table_name = 'uscourts_courts'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    sites = {
        'NY': 'https://www.uscourts.gov/fedcf-query?query={%22by%22:%22location%22,%22page%22:0,%22description%22:%22New%20York,%20NY,%20USA%22,%22county%22:%22New%20York%22,%22state%22:%22NY%22,%22zip%22:%2210007%22,%22country%22:%22US%22,%22locationType%22:%22other%22,%22lat%22:40.7127503,%22lng%22:-74.00597649999997,%22filters%22:%22default%22}',
        'BK': 'https://www.uscourts.gov/fedcf-query?query={%22by%22:%22location%22,%22page%22:0,%22description%22:%22Brooklyn,%20NY,%20USA%22,%22county%22:%22Kings%22,%22state%22:%22NY%22,%22zip%22:%2211216%22,%22country%22:%22US%22,%22locationType%22:%22other%22,%22lat%22:40.6781281,%22lng%22:-73.94416899999999,%22filters%22:%22default%22}'
    }
    def get_location(boro):
        response = requests.get(sites.get(boro)).content
        locations = json.loads(response).get('results').get('locations')
        return locations

    data = get_location('NY') + get_location('BK')

    # deduplicate, --> in reality there's no duplicated items
    def removeduplicate(it):
        seen = []
        for x in it:
            t = tuple(x.items())
            if t not in seen:
                yield x
                seen.append(t)

    data = list(removeduplicate(data))
    ETL(data)