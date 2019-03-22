import sys
sys.path.append('../')
sys.path.append('../..')
import os

import datetime

from dataflows import *
from tools.rename_field import rename_field
from tools.joined_lower import joined_lower

def ETL():

    table_name = 'doe_universalprek'
    url = 'https://data.cityofnewyork.us/api/views/kiyv-ks3f/rows.csv?accessType=DOWNLOAD'
    date = datetime.date.today().isoformat()

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        add_metadata(name=date, title=f'{table_name}.csv'),
        # dump_to_path(date),
        # dump_to_sql(tables={table_name: {'resource-name': table_name}},
        #             engine='env://DATAFLOWS_DB_ENGINE')
        ).process()

if __name__ == '__main__':
    ETL()
