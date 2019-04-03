from dataflows import Flow, dump_to_sql
from lib.dump.postgis_dumper import PostgisDumper
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

def flow(parameters):
    return Flow(
        PostgisDumper(
            resource = parameters.get('resource', None),
            db_table_name = parameters.get('db_table_name', None),
            srcSRS = parameters.get('srcSRS', 'EPSG:4326'), 
            dstSRS = parameters.get('dstSRS', 'EPSG:4326'),
            engine = parameters.get('engine', 'env://DPP_DB_ENGINE')
        )
    )

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)