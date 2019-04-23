from dataflows.helpers.resource_matcher import ResourceMatcher

def get_resource(key, resources=None):
    
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                print(resource[key])
              
        yield package.pkg
        for res in package:
            yield res
            
    return func