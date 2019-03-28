from dataflows import *
from lib.joined_lower import joined_lower
import datetime
import os

def ETL():

    table_name = 'doe_universalprek'
    url = 'https://data.cityofnewyork.us/api/views/kiyv-ks3f/rows.csv?accessType=DOWNLOAD'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        add_metadata(agency='doe'),
        dump_to_path(path),
        # dump_to_sql(tables={table_name: {'resource-name': table_name}},
        #             engine='env://DATAFLOWS_DB_ENGINE')
        ).process()

if __name__ == '__main__':
    ETL()
