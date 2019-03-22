from dataflows import *
from tools.joined_lower import joined_lower

def ETL():
    table_name = 'bic_tradewaste'
    url = 'https://data.cityofnewyork.us/api/views/hsjb-p5ky/rows.csv'
    Flow(
        load(url, name=table_name, format='csv', ),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        joined_lower(resources=table_name),
        dump_to_path(table_name),
        dump_to_sql(tables={table_name: {'resource-name': table_name}},
                    engine='env://DATAFLOWS_DB_ENGINE')
    ).process()

if __name__ == '__main__':
    ETL()

# docker run - -name psql - p 5432: 5432 - e POSTGRES_PASSWORD = 0312 - d postgres