import os
import logging
import boto3
import mimetypes
from dataflows.processors.dumpers.file_dumper import FileDumper

def generate_path(file_path, base_path='', datapackage={}):
    # format_params = {'version': 'latest'}
    # format_params.update(datapackage)
    # try:
    #     base_path = base_path.format(**format_params)
    # except KeyError:
    #     logging.exception('datapackage.json is missing a property')
    #     raise
    return os.path.join(base_path, file_path)

class S3Dumper(FileDumper):

    # def initialize(self, params):
    #     super(S3Dumper, self).initialize(params)
    #     self.bucket = params['bucket']
    #     self.acl = params.get('acl', 'public-read')
    #     self.endpoint_url = (params.get('endpoint_url') or
    #                          os.environ.get('S3_ENDPOINT_URL') or
    #                          'https://s3.amazonaws.com')
    #     self.client = boto3.client('s3', endpoint_url=self.endpoint_url)
    #     self.base_path = params.get('path', '')
    #     self.content_type = params.get('content_type')
    #     self.add_filehash_to_path = params.get('add-filehash-to-path')

    def __init__(self, params, **options):
        super(S3Dumper, self).__init__(options)
        self.bucket = params['bucket']
        self.acl = params.get('acl', 'public-read')
        self.endpoint_url = (params.get('endpoint_url') or
                             os.environ.get('S3_ENDPOINT_URL') or
                             'https://s3.amazonaws.com')
        self.client = boto3.client('s3', endpoint_url=self.endpoint_url)
        self.base_path = params.get('path', '')
        self.content_type = params.get('content_type')
        self.add_filehash_to_path = params.get('add-filehash-to-path')

    def prepare_datapackage(self, datapackage, params):
        super(S3Dumper, self).prepare_datapackage(datapackage, params)
        self.datapackage = datapackage
        return datapackage

    def write_file_to_output(self, filename, path, allow_create_bucket=True):
        key = generate_path(path, self.base_path, self.datapackage)
        content_type, _ = mimetypes.guess_type(key)
        try:
            objs = self.client.list_objects_v2(Bucket=self.bucket, Prefix=key)
            if (not path.endswith('datapackage.json')) and \
                    objs.get('KeyCount') and \
                    self.add_filehash_to_path:
                logging.warning(
                    'Skipping upload of file %s as it already exists', path)
                return
            self.put_object(
                ACL=self.acl,
                Body=open(filename, 'rb'),
                Bucket=self.bucket,
                ContentType=self.content_type or content_type or 'text/plain',
                Key=key)
            return os.path.join(self.endpoint_url, self.bucket, key)
        except self.client.exceptions.NoSuchBucket:
            if os.environ.get("S3_ENDPOINT_URL") and allow_create_bucket:
                # if you provided a custom endpoint url, we assume you are
                # using an s3 compatible server, in this case, creating a
                # bucket should be cheap and easy, so we can do it here
                self.client.create_bucket(Bucket=self.bucket)
                return self.write_file_to_output(filename, path,
                                                 allow_create_bucket=False)
            else:
                raise

    def put_object(self, **kwargs):
        return self.client.put_object(**kwargs)

