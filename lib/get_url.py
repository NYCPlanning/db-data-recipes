from pathlib import Path
import os
from lib.s3.make_client import make_client

# Given recipe and version, return the url to the datapackage.json
def get_url(recipe, version):
    bucket = os.environ.get('BUCKET')
    client = make_client()

    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    objs = client.list_objects_v2(Bucket=bucket, Prefix=f'recipes/{recipe}').get('Contents') 
    versions = [obj['Key'] for obj in sorted(objs, key=get_last_modified)]
    
    try: 
        if version == 'latest':
            dpkg = list(filter(lambda x: Path(x).parts[3] == 'datapackage.json', versions))[0]
        else: 
            dpkg = list(filter(lambda x: (Path(x).parts[2] == version)\
                    and (Path(x).parts[3] == 'datapackage.json'), versions))[0]
        url = os.path.join(os.environ.get('S3_ENDPOINT_URL'),\
                        bucket, dpkg)
        return url
    except:
        assert 'url' in locals(), f'\n\
                {recipe} version {version} not found, \n\
                do "cook recipe ls {recipe}" \n\
                to check whats available\n'
