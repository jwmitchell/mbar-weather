# Module of weather utilities to be used with Synoptic API and sqlite
#
import weather_config
import urllib.request as req
import requests
import os.path
import json
import sqlite3    #needs pip install
import pickle
import datetime
import pytz
import zulu       #Needs pip install

api = weather_config.config['Default']['API_ROOT']
token = weather_config.config['Default']['API_TOKEN']
units = weather_config.config['Default']['UNITS']

def get_base_api_request_url(query_type):
    query_type_address = ''
    if query_type == 'timeseries':
        query_type_address = 'stations/' + query_type
    elif query_type == 'station':
        query_type_address = 'stations/metadata'    
    else:
        raise ValueError('Invalid query type: ' + query_type)
        
    base_api_request_url = os.path.join(api, query_type_address)
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
        raise TypeError(errstr)
    return sqltype
                         
    
def get_example_radius_dataset():
    radius = (38.09,-122.65,3)
    st_radius = ",".join(map(str,radius))
    api_arguments = {"token":token,"start":"201910092300","end":"201910100400","radius":st_radius,"units":units}
    api_request_url = get_base_api_request_url("timeseries")
    req = requests.get(api_request_url, params=api_arguments)
    data = req.json()
    return(data)

def get_station_by_stid(stid,db_object):
# Get the station by id from the database, and provide it as a standard format
# dictionary. If it is not found, get it from the Synoptic API.
# To eliminate nesting differences, fetch the final station data from the database once
# it has been entered.
    station = db_object.get_station(stid)
    rc = 0
    if station == {}:
        # Call API to find station
        api_request_url = get_base_api_request_url('station')
        api_arguments = {'token':token,'stid':stid,'sensorvars':1}
        req = requests.get(api_request_url, params=api_arguments)
        station = req.json()
        rc = station['SUMMARY']['RESPONSE_CODE']
        if rc == 2:
            estr = "stid " + stid + " is not a valid station"
            raise ValueError(estr)
        db_object.add_station(station)
        station = db_object.get_station(stid)    # Ensures same format for existing & new
    return(station)

def get_observations_by_stid_datetime(stid,firstdt,lastdt,db_object):
    # Get a set of observations from a specified weather station in a specified time range.
    # Zulu time is used for the timestamps. 
    # Checks whether the database contains a complete record. If not, re-fills from time range.

    get_station_by_stid(stid,db_object)
    obs = db_object.get_observations(stid,firstdt,lastdt)
    firzdt = TimeUtils(firstdt)
    laszdt = TimeUtils(lastdt)
    needsapi = False
    if obs != []:
        nobs = len(obs)
        if nobs > 1:
            secobtime = obs[1]['DATE_TIME']
            secztime = TimeUtils(secobtime)
            tdelta = secztime.datetime - firzdt.datetime
            trange = laszdt.datetime - firzdt.datetime 
            ticks = int(trange.seconds / tdelta.seconds)
            difobsticks = abs(nobs-ticks)
            if difobsticks > 1:
                needsapi = True
    else:
        needsapi = True
        
    if needsapi:
        # There are missing observations within the time range.
        # Call the API to get missing data over the entire range.
        api_arguments = {"token":token,"start":firzdt.synop(),"end":laszdt.synop(),"stid":stid,"units":units}
        api_request_url = get_base_api_request_url("timeseries")
        req = requests.get(api_request_url, params=api_arguments)
        data = req.json()
        db_object.add_observations(data)
        obs = db_object.get_observations(stid,firstdt,lastdt)

    return(obs)

class TimeUtils(object):

    # The TimeUtils class handles coversion between synoptic API (YYYYMMDDHHSS, UTC), synoptic data
    # (Zulu), and local tuple data ((y,m,d,h,m), Pacific). Data will be stored as a Zulu object.

    def __init__(self, timeobj):

        self.datetime = None
        if isinstance(timeobj,datetime.datetime):
            self.datetime = zulu.Zulu.fromdatetime(timeobj) 
        elif isinstance(timeobj,tuple):
            dt = datetime.datetime(*timeobj)
            self.datetime = zulu.Zulu.fromdatetime(dt)           
        elif isinstance(timeobj,zulu.zulu.Zulu):
            self.datetime = timeobj
        elif isinstance(timeobj,str):
            try:     #Zulu string
                self.datetime = zulu.parse(timeobj)
            except zulu.parser.ParseError:  # Now try synopt string
                try:
                    dt = datetime.datetime(int(timeobj[0:4]),int(timeobj[4:6]),int(timeobj[6:8]),
                                           int(timeobj[8:10]),int(timeobj[10:12]))
                    self.datetime = zulu.Zulu.fromdatetime(dt)
                except AttributeError:
                    print("Invalid time string format for " + timeobj)

    def synop(self):
        # synoptic string is YYYYMMDDHHMM
        ztpl = self.datetime.utctimetuple()
        synopstr = str(ztpl[0]) + str(ztpl[1]).zfill(2) + str(ztpl[2]).zfill(2) + \
            str(ztpl[3]).zfill(2) + str(ztpl[4]).zfill(2)
        return synopstr

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
        sql = 'CREATE TABLE observations (date_time TEXT,' 
        iv = 0
        vlen = len(radius_data['STATION'][0]['OBSERVATIONS'].keys())
        for v in radius_data['STATION'][0]['OBSERVATIONS'].keys():
            iv +=1
            sqltype = python_to_sql(radius_data['STATION'][0]['OBSERVATIONS'][v][0])
            print("Observation var: " + v + "   SQL Type: " + sqltype)
            comma = ', '
            if sqltype != 'REFERENCE' and v != 'date_time':
                sql = sql + v.lower() + '  ' + sqltype + comma
        
        sql = sql + ' volt_set_1 REAL, stid TEXT NOT NULL, PRIMARY KEY (stid, date_time), FOREIGN KEY (stid) REFERENCES station (stid) );'
        print(sql)
        try:
            mydb.cursor.execute(sql)
        except Error as e:
            print(e)

        mydb.connection.commit()
        return mydb

    create = staticmethod(create)

    def add_station(self,data):
        starr = []
        #Determine nesting of data structure containing station data and pack into array
        if data == None:
            raise ValueError('No station data has been provided')
        try:
            starr = data['STATION']    #Station array
        except KeyError:
            try:
                sttest = data['SID']
                starr = [data]         #Single station data
            except KeyError:
                print("Unrecognized station data structure")           
        for st in starr:
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

    def get_station(self, stid):
        sql = 'SELECT * FROM station WHERE stid = \'' + stid + '\';'
        self.cursor.execute(sql)
        sttuple = self.cursor.fetchone()
        stdict = {}
        if sttuple != None:        # Station already exists in database  
            # sqlite returns data in a tuple format. This needs to be converted into the
            # standard dictionary format used by synoptic. Keys are also in CAPS.    
            attr = 0
            for stk in self.cursor.description:
                stkey = stk[0].upper()
                stdict[stkey] = sttuple[attr]
                attr += 1
            # The SENSOR_VARIABLES data were packed as a binary and need to be unpickled       
            stdict['SENSOR_VARIABLES'] = pickle.loads(stdict['SENSOR_VARIABLES'])
        return stdict        

    def add_observations(self,data):
        #Determine nesting of data structure containing station data and pack into array
        if data == None or data == {}:
            raise ValueError('No observation data has been provided')
        try:
            starr = data['STATION']    #Station array
        except KeyError:
            try:
                sttest = data['SID']
                starr = [data]         #Single station data
            except KeyError:
                print("Unrecognized station data structure")           
        for station in data['STATION']:
            sql = 'INSERT OR IGNORE INTO observations ('
            stid = station['STID']
            stdat = self.get_station(stid)
            obar = []
            qm = ''
            if stdat == {}:
                self.add_station(data)
            for i in range(len(station['OBSERVATIONS']['date_time'])):
                obtuple = ()
                for okey in station['OBSERVATIONS'].keys():
                    var = okey.lower()
                    val = (station['OBSERVATIONS'][okey][i],)
                    if i == 0:
                        sql = sql + var + ','
                        qm = qm + '?,'
                    obtuple = obtuple + val
                obtuple = obtuple + (stid,)
                obar.append(obtuple)
            qm.strip(',')
            sql = sql + 'stid) VALUES('+ qm + '?);'
            print(sql)
            self.connection.executemany(sql,obar)
        self.connection.commit()

    def get_observations(self,stid,dtlow,dthigh):
        sql = f'''SELECT * FROM observations WHERE stid = \'{stid}\' AND \
        date_time BETWEEN \'{dtlow}\' AND \'{dthigh}\' \
        ;'''
        self.cursor.execute(sql)
        obstuplist = self.cursor.fetchall()
        oblist = []
        if len(obstuplist) != 0:        # Found some observations in database  
            # sqlite returns data in a tuple format. This needs to be converted into the
            # standard dictionary format used by synoptic. Keys are also in CAPS.
            # Also, synoptic returns a dict of lists for each variable. In order to provide
            # atomic time data, weather_utils converts this to a list of dicts, so that all
            # data associated with a time stamp are attributes of a dict.
            for obtup in obstuplist:
                attr = 0
                obdict = {}
                for obk in self.cursor.description:
                    obkey = obk[0].upper()
                    obdict[obkey] = obtup[attr]
                    attr += 1
                oblist.append(obdict)
        return oblist
        
    
    def close(self):
        self.db_name = None
        self.cursor = None
        self.connection.close()

    
if __name__ == '__main__':

    radius_data = get_example_radius_dataset()    
    mydb0 = WeatherDB.create("test_example.db")
    #    mydb0.add_station(radius_data)
    #    station = get_station_by_stid('PG133',mydb0)
    #    print(station)
    #    station = get_station_by_stid('PG130',mydb0)
    #    print(station)
    #    station = get_station_by_stid('Bogus',mydb0)
    mydb0.add_observations(radius_data)
    obs = mydb0.get_observations('PG133','2019-10-09T23:11:00Z','2019-10-10T03:11:00Z')
    obs2 = get_observations_by_stid_datetime('PG133','2019-10-09T23:11:00Z','2019-10-10T01:11:00Z',mydb0)
    obs3 = get_observations_by_stid_datetime('PG133','2019-10-09T23:11:00Z','2019-10-10T11:11:00Z',mydb0)

    mydb0.close()
