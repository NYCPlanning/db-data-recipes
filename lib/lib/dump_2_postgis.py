from dataflows import Flow, dump_to_sql
from lib.dump.postgis_dumper import PostgisDumper
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

def flow(parameters):
    return Flow(
        PostgisDumper(
            resources=None,
            db_table_name=None,
            srcSRS='EPSG:4326', 
            dstSRS='EPSG:4326',
            engine='env://DPP_DB_ENGINE'
        )
    )

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)