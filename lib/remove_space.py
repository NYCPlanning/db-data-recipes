from dataflows.helpers.resource_matcher import ResourceMatcher
import re

def rename_in_row(rows):
    for row in rows:
        row = dict((re.sub(r'[,\_\-\ \.\&]', '', k.replace('#','num').replace("'s","").replace("’s","")), v) for k, v in row.items())
        yield row

def remove_space(resources=None):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                for i in resource['schema']['fields']:
                    i['name'] = re.sub(r'[,\_\-\ \.\&]', '', i['name'].replace('#','num').replace("'s","").replace("’s",""))

        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield rename_in_row(res)
            else:
                yield res
    return func

def rename_in_row_lower(rows):
    for row in rows:
        row = dict((re.sub(r'[,\_\-\ \.\&]', '', k.lower().replace('#','number').replace("'s","")), v) for k, v in row.items())
        yield row

def remove_space_lower(resources=None):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                for i in resource['schema']['fields']:
                    i['name'] = re.sub(r'[,\_\-\ \.\&]', '', i['name'].lower().replace('#','number').replace("'s",""))

        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield rename_in_row_lower(res)
            else:
                yield res
    return func

    