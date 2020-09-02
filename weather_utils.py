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

    cc = connection.cursor()
    # Get example dataset
    radius_data = get_example_radius_dataset()
    print("Creating tables for " + db_path_name)

    # Create table for UNITS
    cc.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='units' ''')
    if cc.fetchone()[0] ==1:        #Error if table exists
        raise RuntimeError("SQL table units already exists")
    cc.execute('''CREATE TABLE units (
    variable text NOT NULL,
    units text NOT NULL
    );''')
    sql = 'INSERT INTO units(variable,units) VALUES(?,?)'
    values =[]
    for vv,uu in radius_data['UNITS'].items():
        values.append((vv,uu))
    cc.executemany(sql,values)

    # Create station table
    cc.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='station' ''')
    if cc.fetchone()[0] ==1:        #Error if table exists
        raise RuntimeError("SQL table station already exists")
    sql = 'CREATE TABLE station (stid TEXT PRIMARY KEY, '
    iv = 0
    vlen = len(radius_data['STATION'][0].keys())
    for v in radius_data['STATION'][0].keys():
        iv +=1
        sqltype = python_to_sql(radius_data['STATION'][0][v])
        print("Station var: " + v + "   SQL Type: " + sqltype)
        comma = ', '
        if sqltype != 'REFERENCE' and v not in ['STID','OBSERVATIONS']:
            sql = sql + v.lower() + '  ' + sqltype
        else:
            if v in ['OBSERVATIONS','STID']:    #STID is PRIMARY KEY. OBSERVATIONS is foreign table with STID as a foreign key
                comma = ''
            elif v == 'SENSOR_VARIABLES':
                sql = sql + v.lower() + ' BLOB  NOT NULL'
            elif v == 'PERIOD_OF_RECORD':
                por_start =  radius_data['STATION'][0]['PERIOD_OF_RECORD']['start']
                por_end =  radius_data['STATION'][0]['PERIOD_OF_RECORD']['end']
                sql = sql + 'period_of_record_start TEXT  NOT NULL, '
                sql = sql + 'period_of_record_stop TEXT  NOT NULL'
            else:
                raise ValueError("Unknown station reference variable " + v)

        if iv != vlen:
            sql = sql + comma
        
    sql = sql + ');'
    print(sql)
    try:
        cc.execute(sql)
    except ERROR as e:
        print(e)
    connection.commit()

def python_to_sql(obj):
    sqltype = 'NULL'
    if (isinstance(obj,str)):
        sqltype = 'TEXT'
    elif (isinstance(obj,int)):
        sqltype = 'INTEGER'
    elif (isinstance(obj,float)):
        sqltype = 'REAL'
    elif (isinstance(obj,dict)):
        sqltype = 'REFERENCE'   # Not a real sql type, foreign key
    else:
        raise ValueError("Type " + type(obj) + " is not a recognized SQL type")
    return sqltype
                         
    
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
    


