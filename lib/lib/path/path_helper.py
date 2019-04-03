from pathlib import Path
import datetime
import os
from urllib.parse import urlparse, urljoin

def create_s3_path(bucket, path): 
    # gets base url, then paste it with dataset directory
    s3 = os.environ.get('S3_ENDPOINT_URL').split('//')
    base_url = f'{s3[0]}//{bucket}.{s3[1]}'
    s3_path = urljoin(base_url, path)
    return s3_path

def create_csv_path(path):
    new_path = Path(path)/ f'{datetime.date.today().isoformat()}'
    return str(new_path)