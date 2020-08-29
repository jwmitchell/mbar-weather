# Module of weather utilities to be used with Synoptic API and sqlite
#
import weather_config
import urllib.request as req
import requests
import os.path
import json
import sqlite3

def get_base_api_request_url(query_type):
    query_type_address = ''
    if query_type == 'timeseries':
        query_type_address = 'stations/' + query_type
    else:
        raise ValueError('Invalid query type: ' + query_type)
        
    base_api_request_url = os.path.join(api, "stations/timeseries")
    return base_api_request_url

api = weather_config.config['Default']['API_ROOT']
token = weather_config.config['Default']['API_TOKEN']
units = weather_config.config['Default']['UNITS']

radius = (38.09,-122.65,10)
st_radius = ",".join(map(str,radius))

api_arguments = {"token":token,"start":"201910092300","end":"201910101700","radius":st_radius,"units":units}

api_request_url = get_base_api_request_url("timeseries")

req = requests.get(api_request_url, params=api_arguments)

# response = req.urlopen(api_request_url, api_arguments)

data = req.json()

print(data)

def date_to_int(date_string):
    print ("date_string")
    


