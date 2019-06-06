from dataflows import *
from lib import joined_lower, create_base_path
from lib import dump_to_s3
import requests
from bs4 import BeautifulSoup
import urllib.request

def ETL(data):
    table_name = 'hra_centers'
    base_path = create_base_path(__file__)

    Flow(
        data,
        update_resource(None, name=table_name),
        update_resource(resources=table_name, path=table_name+'.csv'),
        joined_lower(resources=table_name),
        dump_to_s3(resources=table_name, params=dict(base_path=base_path))
    ).process()

if __name__ == "__main__":
    hra_centers = {"Supplemental Nutrition Assistance Program (SNAP)": "https://www.google.com/maps/d/kml?mid=1GBr0_gE1VBUtJZRLJosLSl1B9Xo&forcekml=1",
        "Community Alternative Systems Agency (CASA)": "https://www.google.com/maps/d/kml?mid=1hpyh201fVvv_p6oZKa6IOP8zzeebRy5p&forcekml=1",
        "HIV/AIDS Services Administration (HASA)": "https://www.google.com/maps/d/kml?mid=1GIYqt96Cbq-ppWyvVT0iJRLnbSk&forcekml=1",
        "Job Center": "https://www.google.com/maps/d/kml?mid=1uC3LcicVmGp_CZcyQZkGuf8rLGY&forcekml=1",
        "Medicaid Office": "https://www.google.com/maps/d/kml?mid=1Ypu_qfcW7jBGDA_2uK1xP3TwUTw&forcekml=1",
        "Office of Child Support Services (OCSS)": "https://www.google.com/maps/d/kml?mid=1MYy3WOFBNgJ4D4-rqMYoXs2R3dk&forcekml=1"
        }

    data = []
    for center in hra_centers.keys():
        html_content = urllib.request.urlopen(hra_centers[center])
        soup = BeautifulSoup(html_content, 'html.parser')
        descriptions = soup.find_all('description')
        facility = soup.find_all('name')   
        for d in range(1,len(descriptions)):
            item = descriptions[d].text
            item = item.split('<br>')
            result = {}
            fclty = facility[d+1].text
            for i in range(len(item)):
                result['facility_name'] = fclty
                result['type'] = center
                parse = item[i].split(': ')
                key = parse[0]
                value = parse[1]
                if key == 'Location Address': 
                    key = 'Address'
                if key == 'Zip Code': 
                    key = 'Zipcode'
                result[key] = value
            data.append(result)

    ETL(data)