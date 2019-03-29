from dataflows import *
from lib.joined_lower import joined_lower
import datetime
import os
from lib import dump_to_s3

def ETL():
    table_name = 'bic_tradewaste'
    url = 'https://data.cityofnewyork.us/api/views/hsjb-p5ky/rows.csv'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())
    Flow(
        load(url, name=table_name, format='csv', ),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        joined_lower(resources=table_name),
        dump_to_path(path, add_filehash_to_path=True),
        # dump_to_s3(resources=table_name, params=dict(bucket='sptkl', path=path))
        dump_to_sql(tables={table_name: {'resource-name': table_name}},
                    engine='env://DATAFLOWS_DB_ENGINE')
    ).process()

if __name__ == '__main__':
    ETL()