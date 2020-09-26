# Module of weather utilities to be used with Synoptic API and sqlite
#
import weather_config
import urllib.request as req
import requests
import os.path
import json
import sqlite3
import pickle

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
        errstr = "Type " + str(type(obj)) + " is not a recognized SQL type"
        raise ValueError(errstr)
    return sqltype
                         
    
def get_example_radius_dataset():
    radius = (38.09,-122.65,3)
    st_radius = ",".join(map(str,radius))

    api_arguments = {"token":token,"start":"201910092300","end":"201910100400","radius":st_radius,"units":units}
    api_request_url = get_base_api_request_url("timeseries")
    req = requests.get(api_request_url, params=api_arguments)
    data = req.json()
    return(data)

def get_station_by_stid(stid,db_name):
# Get the station by id from the database, and provide it as a standard format
# dictionary.
    
    if not os.path.isfile(db_name):
        raise FileExistsError(db_name + " does not exist")
    try:
        connection = sqlite3.connect(db_name)
    except Error as e:
        print(e)
    cc = connection.cursor()
    sql = 'SELECT * FROM station WHERE stid = \'' + stid + '\';'

    cc.execute(sql)
    sttuple = cc.fetchone()

    if sttuple != None:        # Station already exists in database
    
        # sqlite returns data in a tuple format. This needs to be converted into the
        # standard dictionary format used by synoptic. Keys are also in CAPS.
    
        stdict = {}
        attr = 0
        for stk in cc.description:
            stkey = stk[0].upper()
            stdict[stkey] = sttuple[attr]
            attr += 1
            
        # The SENSOR_VARIABLES data were packed as a binary and need to be unpickled
            
        stdict['SENSOR_VARIABLES'] = pickle.loads(stdict['SENSOR_VARIABLES'])
            
        print(stdict)

    else:
        estring = str(stid) + ' is not a valid station'
        raise ValueError(estring)

    return stdict

    
    ## Next steps: check for null tuple, if null find API call for station, add new station.

class WeatherDB(object):

    # Class WeatherDB handles all operations on the weather data caching database. It makes
    # persistent connections available without requiring open and close operations.
    # Because sqlite will create a file if passed a filename that does not exist, there is
    # the potential for accidentally creating a database when one was to be opened. To prevent
    # this contingency, the creation of a WeatherDB object must be performed via the static
    # method WeatherDB.create(newdbfilename), which returns a WeatherDB object. Binding a
    # WeatherDB object to an existing database simply uses the constructor
    # WeatherDB(existingdbfilename).

    def __init__(self,db_name):
        if not os.path.isfile(db_name):
            raise FileExistsError(db_name + " does not exist, use WeatherDB.create(db_name)")

        print("Opening " + db_name)
        try:
            connection = sqlite3.connect(db_name)
        except Error as e:
            print(e)

        self.connection = connection
        self.cursor = connection.cursor()
        self.db_name = db_name
        
    def create(db_name):
        if os.path.isfile(db_name):
            raise ValueError(db_name + " already exists. Use WeatherDB(db_name).")
        print("Creating " + db_name)
        db_file = open(db_name,'w')
        db_file.close()
        mydb = WeatherDB(db_name)
        
        # Get example dataset - Currently this is dynamically pulled from Synoptic, probably
        # should be static data
        radius_data = get_example_radius_dataset()
        print("Creating tables for " + db_name)

        # Create table for UNITS
        mydb.cursor.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='units' ''')
        if mydb.cursor.fetchone()[0] ==1:        #Error if table exists
            raise RuntimeError("SQL table units already exists")
        sql = "CREATE TABLE units (variable text NOT NULL,units text NOT NULL);"
        mydb.cursor.execute(sql)
        sql = 'INSERT INTO units(variable,units) VALUES(?,?)'
        values =[]
        for vv,uu in radius_data['UNITS'].items():
            values.append((vv,uu))
            mydb.cursor.executemany(sql,values)

        # Create station table
        mydb.cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='station'")
        if mydb.cursor.fetchone()[0] ==1:        #Error if table exists
            raise RuntimeError("SQL table station already exists")
        sql = "CREATE TABLE station (sid INTEGER PRIMARY KEY, "
        iv = 0
        vlen = len(radius_data['STATION'][0].keys())
        for v in radius_data['STATION'][0].keys():
            iv +=1
            sqltype = python_to_sql(radius_data['STATION'][0][v])
            print("Station var: " + v + "   SQL Type: " + sqltype)
            comma = ', '
            if sqltype != 'REFERENCE' and v not in ['OBSERVATIONS']:
                sql = sql + v.lower() + '  ' + sqltype
            else:
                if v == 'OBSERVATIONS':    #OBSERVATIONS is foreign table with STID as a foreign key
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
        
        sql = sql + ', UNIQUE(stid));'
        print(sql)
        try:
            mydb.cursor.execute(sql)
        except Error as e:
            print(e)

        # Create observations table
        mydb.cursor.execute('''SELECT count(name) FROM sqlite_master WHERE type='table' AND name='observations' ''')
        if mydb.cursor.fetchone()[0] ==1:        #Error if table exists
            raise RuntimeError("SQL table observations already exists")
        sql = 'CREATE TABLE observations (date_time TEXT PRIMARY KEY, '
        iv = 0
        vlen = len(radius_data['STATION'][0]['OBSERVATIONS'].keys())
        for v in radius_data['STATION'][0]['OBSERVATIONS'].keys():
            iv +=1
            sqltype = python_to_sql(radius_data['STATION'][0]['OBSERVATIONS'][v][0])
            print("Observation var: " + v + "   SQL Type: " + sqltype)
            comma = ', '
            if sqltype != 'REFERENCE' and v != 'date_time':
                sql = sql + v.lower() + '  ' + sqltype + comma
        
        sql = sql + ' stid TEXT NOT NULL, FOREIGN KEY (stid) REFERENCES station (stid) );'
        print(sql)
        try:
            mydb.cursor.execute(sql)
        except Error as e:
            print(e)

        mydb.connection.commit()
        return mydb

    create = staticmethod(create)

    def add_station(self,data):

        if data['STATION'] == None:
            raise ValueError('No station data has been provided')

        for st in data['STATION']:
            sql = 'INSERT INTO station('
            dbtuple = ()
            qm = ''
            for sd in st.keys():
                if sd not in ['OBSERVATIONS','PERIOD_OF_RECORD','SENSOR_VARIABLES']:
                    var = sd.lower()
                    val = (st[sd],)
                    sql = sql + var + ','
                    dbtuple = dbtuple + val
                    qm = qm + '?,'
                elif sd == 'PERIOD_OF_RECORD':
                    por_start =  st[sd]['start']
                    por_end = st[sd]['end']
                    sql = sql + 'period_of_record_start, period_of_record_stop,'
                    dbtuple = dbtuple + (por_start,por_end)
                    qm = qm + '?,?,'
                elif sd == 'SENSOR_VARIABLES':
                    var = sd.lower()                
                    sql = sql + var + ','
                    qm = qm + '?,'
                    pdat = pickle.dumps(st[sd],pickle.HIGHEST_PROTOCOL)
                    val = (pdat,)
                    dbtuple = dbtuple + val

            sql = sql.rstrip(',')
            qm = qm.strip(',')
            sql = sql + ') VALUES('+ qm + ')'

        self.cursor.execute(sql,dbtuple)
        self.connection.commit()

    def close(self):
        self.db_name = None
        self.cursor = None
        self.connection.close()

    
if __name__ == '__main__':

#    get_station_by_stid('PG133',"test_example.db")

    radius_data = get_example_radius_dataset()    
    mydb0 = WeatherDB.create("test_example.db")
    mydb0.add_station(radius_data)
#    mydb1 = WeatherDB("test_example.db")
    mydb0.close()



