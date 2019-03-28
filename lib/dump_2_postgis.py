from dataflows import Flow, dump_to_sql
from .dump.to_postgis import dumper
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

def flow(parameters):
    return Flow(
        dumper(
            parameters['tables'], #resources
            engine=parameters.get('engine', 'env://DPP_DB_ENGINE') #engine
        )
    )

if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)