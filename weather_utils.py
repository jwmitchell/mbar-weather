# Module of weather utilities to be used with Synoptic API and sqlite
#
import weather_config
import urllib.request as req
import requests
import os.path
import json
import sqlite3

api = weather_config.config['Default']['API_ROOT']
token = weather_config.config['Default']['API_TOKEN']
units = weather_config.config['Default']['UNITS']

def get_base_api_request_url(query_type):
    query_type_address = ''
    if query_type == 'timeseries':
        query_type_address = 'stations/' + query_type
    else:
        raise ValueError('Invalid query type: ' + query_type)
        
    base_api_request_url = os.path.join(api, "stations/timeseries")
    return base_api_request_url

def create_weather_db(db_path_name):
    if db_path_name == '':
        db_path_name = 'example.db'
    connection = None
    try:
        connection = sqlite3.connect(db_path_name)
    except Error as e:
        print(e)

    c = connection.cursor()
    # Get example dataset
    radius_data = get_example_radius_dataset()
    print("Creating tables for " + db_path_name)

    # Create table for UNITS
    c.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='units' ''')
    if c.fetchone()[0] !=1:        #Skip if table exists
        c.execute('''CREATE TABLE units (
        variable text NOT NULL,
        units text NOT NULL
        );''')
        sql = 'INSERT INTO units(variable,units) VALUES(?,?)'
        values =[]
        for v,u in radius_data['UNITS'].items():
            values.append((v,u))
        print("loading values")
        c.executemany(sql,values)

        

def get_example_radius_dataset():
    radius = (38.09,-122.65,3)
    st_radius = ",".join(map(str,radius))

    api_arguments = {"token":token,"start":"201910092300","end":"201910101700","radius":st_radius,"units":units}
    api_request_url = get_base_api_request_url("timeseries")
    req = requests.get(api_request_url, params=api_arguments)
    data = req.json()
    return(data)

# radius_data = get_example_radius_dataset()

create_weather_db("test_example.db")
print("finished")



def date_to_int(date_string):
    print ("date_string")
    


