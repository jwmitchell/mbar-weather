# Test using KLAX data from synoptic
#
import configparser
import urllib.request as req
import requests
import os.path
import json

config = configparser.ConfigParser()
config.read('weather.ini')

api = config['Default']['API_ROOT']
token = config['Default']['API_TOKEN']

print ("api, token = " + api + " " + token)

api_request_url = os.path.join(api, "stations/timeseries")

radius = (38.09,-122.65,10)
st_radius = ",".join(map(str,radius))

api_arguments = {"token":token,"start":"201910091500","end":"201910091700","radius":st_radius}


req = requests.get(api_request_url, params=api_arguments)

# response = req.urlopen(api_request_url, api_arguments)

data = req.json()

print(data)
