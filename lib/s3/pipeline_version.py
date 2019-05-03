import os
import sys
from .make_client import make_client, get_last_modified
import datetime
from pathlib import Path
client = make_client()
bucket = os.environ.get('BUCKET')

def get_version(pipeline):
    objs = client.list_objects_v2(Bucket=bucket, Prefix=f'pipelines/{pipeline}').get('Contents')
    versions = set([Path(obj['Key']).parts[2] for obj in sorted(objs, key=get_last_modified)])
    for i in versions:
        print(i)
