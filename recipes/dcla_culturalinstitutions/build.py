import sys
sys.path.append('..')
sys.path.append('../..')


import datetime
from dataflows import *
from tools.joined_lower import joined_lower

def ETL():
    table_name = 'dcla_culturalinstitutions'
    url = 'https://data.cityofnewyork.us/api/views/u35m-9t32/rows.csv?accessType=DOWNLOAD'
    date = datetime.date.today().isoformat()
    Flow(
        load(url, name=table_name, format='csv', validate=True),
        joined_lower(resources=table_name),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        dump_to_path(f'{date}'),
        dump_to_sql(tables={table_name: {'resource-name': table_name}},
                    engine='env://DATAFLOWS_DB_ENGINE')
    ).process()

if __name__ == '__main__':
    ETL()

# add_metadata should come last right before dumping
# export DATAFLOWS_DB_ENGINE=postgresql://postgres:0312@localhost:5432/postgres