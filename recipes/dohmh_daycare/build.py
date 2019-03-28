from dataflows import *
from lib.joined_lower import joined_lower
import datetime
import os

def ETL():

    table_name = 'dohmh_daycare'
    url = 'https://data.cityofnewyork.us/api/views/dsg6-ifza/rows.csv?accessType=DOWNLOAD'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())

    Flow(
        load(url, name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        # add_metadata(name=date, title=f'{table_name}.csv'),
        dump_to_path(path),
        # dump_to_sql(tables={table_name: {'resource-name': table_name}},
        #             engine='env://DATAFLOWS_DB_ENGINE')
        ).process()

if __name__ == '__main__':
    ETL()
