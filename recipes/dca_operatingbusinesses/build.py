from dataflows import *
from lib.joined_lower import joined_lower
import datetime
import os

def ETL():
    table_name = 'dca_operatingbusinesses'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())
    Flow(
        load('https://data.cityofnewyork.us/api/views/w7w3-xahh/rows.csv?accessType=DOWNLOAD',
                name=table_name,
                format='csv',
                validate=True),
        joined_lower(resources=table_name),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        dump_to_path(path, add_filehash_to_path=True),
        # dump_to_sql(tables={table_name: {'resource-name': table_name}},
        #             engine='postgresql://postgres:1234@localhost:5432/postgres')
    ).process()

if __name__ == '__main__':
    ETL()

# add_metadata should come last right before dumping