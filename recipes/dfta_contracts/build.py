import sys
sys.path.append('../')
sys.path.append('../..')
import os

import datetime

from dataflows import *
from lib.rename_field import rename_field
from lib.joined_lower import joined_lower

def ETL():

    table_name = 'dfta_contracts'
    url = 'https://data.cityofnewyork.us/api/views/6j6t-3ixh/rows.csv?accessType=DOWNLOAD'
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
