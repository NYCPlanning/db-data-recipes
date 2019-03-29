from pathlib import Path
import datetime
import os
from urllib.parse import urlparse, urljoin

def create_s3_path(bucket, path): 
    # gets base url, then paste it with dataset directory
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL')
    s3 = urlparse(s3_endpoint_url)
    base_url = urljoin(s3.scheme, f'{bucket}.{s3.netloc}')
    s3_path = urljoin(base_url, path)
    return s3_path

def create_csv_path(path):
    new_path = Path(path).parent / f'{datetime.date.today().isoformat()}'
    return new_path

# def create_dump_path(): 
