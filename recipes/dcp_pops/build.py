import datetime
from dataflows import *
from lib.rename_field import rename_field
from lib.joined_lower import joined_lower
import zipfile
import requests
import shutil
import io
import os

def unzip():
    url = 'https://nycopendata.socrata.com/api/views/fum3-ejky/files/RmDbuOaF2Ix4qRBCywjyAQoeRy55Hz8L2kQHZy8R3Mc?filename=DCP_POPS.zip'
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall('/recipes/dcp_pops/tmp')

def rm_tmp():
    shutil.rmtree('/recipes/dcp_pops/tmp')

def mdb_to_csv():
    os.system('sh /recipes/dcp_pops/mdb_to_csv.sh')

def ETL():

    table_name = 'dcp_pops'
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 
                        datetime.date.today().isoformat())
    Flow(
        load('/recipes/dcp_pops/tmp/tbl_POPS.csv', name=table_name, format='csv', force_strings=False),
        joined_lower(resources=table_name),
        set_type('dcp_record', resources=table_name, type='string'),
        set_type('community_district', resources=table_name, type='string'),
        add_metadata(name=table_name, title=f'{table_name}.csv'),
        # dump_to_path(date),
        dump_to_sql(tables={table_name: {'resource-name': table_name}},
                    engine='env://DATAFLOWS_DB_ENGINE')
        ).process()

if __name__ == '__main__':
    unzip()
    mdb_to_csv()
    ETL()
    rm_tmp()

# docker run - -name psql - p 5432: 5432 - e POSTGRES_PASSWORD = 1234 - d postgres
# docker run --rm -it\
#   -v `pwd`:/pipelines/db-facilities dpp bash
# docker run --rm -it -v `pwd`:/home/db-facilities dpp bash

# for TT in $(mdb-tables DCP_POPS_NYC_BigApps.mdb); do
#      mdb-export -D '%Y-%m-%d %H:%M:%S' DCP_POPS_NYC_BigApps.mdb "$TT" > "${TT}.csv"
# done
