import usaddress
import re
def get_hnum(address): 
        result = [k for (k,v) in usaddress.parse(address) \
                if re.search("Address", v)]
        return ' '.join(result)

def get_sname(address): 
        result = [k for (k,v) in usaddress.parse(address) \
                if re.search("Street", v)]
        return ' '.join(result)

def get_zipcode(address): 
        result = [k for (k,v) in usaddress.parse(address) \
                if re.search("ZipCode", v)]
        return ' '.join(result)