
from dataflows import *
from tools.rename_field import rename_field
from tools.joined_lower import joined_lower
import datetime
import zipfile
import requests
import shutil
import io

def unzip():
    url = 'https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyc-colp-201811.zip'
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall('tmp')

def rm_tmp():
    shutil.rmtree('tmp')

def ETL():

    table_name = 'dcas_colp'
    date = datetime.date.today().isoformat()

    Flow(
        load('tmp/COLP_2018.xlsx', name=table_name, format='xlsx', sheet=1),
        rename_field('MAPPABLE (1=NOT MAPPED)', 'MAPPABLE'),
        joined_lower(resources=table_name),
        set_type('bbl', resources=table_name, type='string'),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        dump_to_path(date),
        # dump_to_sql(tables={table_name: {'resource-name': table_name}},
        #             engine='env://DATAFLOWS_DB_ENGINE')
        ).process()

if __name__ == '__main__':
    unzip()
    ETL()
    rm_tmp()

# docker run - -name psql - p 5432: 5432 - e POSTGRES_PASSWORD = 0312 - d postgres
# docker run --rm -it\
#   -v `pwd`:/pipelines/db-facilities dpp bash