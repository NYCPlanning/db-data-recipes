import itertools
import copy
import logging

import datapackage

from dataflows.helpers.resource_matcher import ResourceMatcher

from datapackage_pipelines.wrapper import ingest, spew, get_dependency_datapackage_url
from datapackage_pipelines.utilities.resources import tabular, PROP_STREAMING, \
    PROP_STREAMED_FROM
from pathlib import Path
import os
from lib.s3.make_client import make_client
import sys
import csv
csv.field_size_limit(sys.maxsize)

def progress_logger(iter, log_progress_rows):
    for i, row in enumerate(iter, 1):
        yield row
        if i % log_progress_rows == 0:
            logging.info('loaded {} rows'.format(i))

class ResourceLoader(object):

    def __init__(self):
        self.parameters, self.dp, self.res_iter = ingest()

    def __call__(self):
        recipe = self.parameters.get('recipe')
        results = ResourceLoader.get_dpkg(recipe)
        url = results.get('url')
        resource = results.get('resource')

        limit_rows = self.parameters.get('limit-rows')
        log_progress_rows = self.parameters.get('log-progress-rows')
        dep_prefix = 'dependency://'
        if url.startswith(dep_prefix):
            dependency = url[len(dep_prefix):].strip()
            url = get_dependency_datapackage_url(dependency)
            assert url is not None, "Failed to fetch output datapackage for dependency '%s'" % dependency
        stream = self.parameters.get('stream', True)
        required = self.parameters.get('required', True)
        resources = self.parameters.get('resources')

        if resource is not None:
            assert not resources
            resource_index = resource if isinstance(resource, int) else None
        else:
            assert resources
            resource_index = None
            resource = list(resources.keys())
        name_matcher = (
            ResourceMatcher(resource, self.dp)
            if isinstance(resource, (str, list))
            else None
        )

        selected_resources = []
        found = False
        try:
            dp = datapackage.DataPackage(url)
        except Exception:
            if required:
                raise
            else:
                dp = None
        if dp:
            dp = self.process_datapackage(dp)
            for i, orig_res in enumerate(dp.resources):
                if resource_index == i or \
                        (name_matcher is not None and name_matcher.match(orig_res.descriptor.get('name'))):
                    found = True
                    desc = copy.deepcopy(orig_res.descriptor)
                    if 'primaryKey' in desc.get('schema', {}):
                        # Avoid duplication checks
                        del orig_res.descriptor['schema']['primaryKey']
                        orig_res.commit()
                    desc[PROP_STREAMED_FROM] = orig_res.source
                    if resources:
                        desc.update(resources[desc['name']])
                    self.dp['resources'].append(desc)
                    if tabular(desc) and stream:
                        desc[PROP_STREAMING] = True
                        orig_res_iter = orig_res.iter(keyed=True)
                        if limit_rows:
                            orig_res_iter = itertools.islice(orig_res_iter, limit_rows)
                        if log_progress_rows:
                            orig_res_iter = progress_logger(orig_res_iter, log_progress_rows)
                        selected_resources.append(orig_res_iter)
                    else:
                        desc[PROP_STREAMING] = False

        assert found or not required, "Failed to find resource with index or name matching %r" % resource
        spew(self.dp, itertools.chain(self.res_iter, selected_resources))

    @staticmethod
    def get_dpkg(recipe):

        bucket = os.environ.get('BUCKET')
        client = make_client()

        resource = recipe.split(':')[0]
        version = recipe.split(':')[1]

        get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
        objs = client.list_objects_v2(Bucket=bucket, Prefix=resource).get('Contents') 
        versions = [obj['Key'] for obj in sorted(objs, key=get_last_modified)]
        
        try: 
            if version == 'latest':
                dpkg = list(filter(lambda x: Path(x).parts[2] == 'datapackage.json', versions))[0]
            else: 
                dpkg = list(filter(lambda x: (Path(x).parts[1] == version)\
                        and (Path(x).parts[2] == 'datapackage.json'), versions))[0]
            url = os.path.join(os.environ.get('S3_ENDPOINT_URL'),\
                            bucket, dpkg)
            return dict(resource=resource, url=url)
        except:
            assert 'url' in locals(), f'\n\
                    {resource}:{version} not found, \n\
                    do "cook recipe ls {resource}" \n\
                    to check whats available\n'


    def process_datapackage(self, dp_):
        return dp_


if __name__ == '__main__':
    ResourceLoader()()