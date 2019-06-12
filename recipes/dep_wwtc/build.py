from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3, get_hnum, get_sname, get_zipcode
from pathlib import Path

def ETL():
    table_name = 'dep_wwtc'
    base_path = create_base_path(__file__)
    file_path = Path(__file__).parent/'dep_wwtc.csv'
    Flow(
        load(str(file_path), name=table_name, format='csv', force_strings=True),
        joined_lower(resources=table_name),
        add_computed_field([dict(target=dict(name = 'zipcode', type = 'string'),
                                operation=lambda row: get_zipcode(row['address'])
                                        ),
                        dict(target=dict(name = 'house_number', type = 'string'),
                                operation = lambda row: get_hnum(row['address'])
                                        ),
                        dict(target=dict(name = 'street_name', type = 'string'),
                                operation=lambda row: get_sname(row['address'])
                                        )
                        ]),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
   ETL()