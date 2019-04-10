import os 
import boto3

def make_client():

    session = boto3.session.Session()
    client = session.client('s3',
                            endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    return client


get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
bucket = os.environ.get('BUCKET')
