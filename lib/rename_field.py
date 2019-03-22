from dataflows.helpers.resource_matcher import ResourceMatcher

def rename_in_row(rows, name, new_name):
    for row in rows:
        row[new_name] = row.pop(name)
        yield row

def rename_field(name, new_name, resources=None):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                for i in resource['schema']['fields']:
                    if i['name'] == name:
                        i['name'] = new_name

        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield rename_in_row(res, name, new_name)
            else:
                yield res

    return func
