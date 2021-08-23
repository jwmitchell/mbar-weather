# Module of weather utilities to be used with Synoptic API and sqlite
#
import weather_config
import urllib.request as req
import requests
import os
import os.path
import json
import random
import sqlite3    #needs pip install
import pickle
import datetime
from datetime import timedelta
import pytz
import zulu       #Needs pip install
import logging

logging.basicConfig(level=weather_config.config['Default']['LOG_LEVEL'])

api = weather_config.config['Default']['API_ROOT']
token = weather_config.config['Default']['API_TOKEN']
units = weather_config.config['Default']['UNITS']
db_schema_raw = weather_config.config['Schema']['DB_SCHEMA']
db_schema = json.loads(db_schema_raw)

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
    api_arguments = {"token":token,"start":"201910092300","end":"201910100400","radius":st_radius,"units":"metric"}
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

def get_observations_by_radius_datetime(latitude,longitude,radius,firstdt,lastdt,db_object):
    # This will return all observations within the radius and time window. Will check for existence
    # in database first. The time variables firstdt and lastdt are TimeUtils objects.

    obsdb = check_db_radius_datetime(latitude,longitude,radius,firstdt,lastdt,db_object) # Stubbed

    if obsdb == False:
        georadius = (latitude,longitude,radius)
        st_radius = ",".join(map(str,georadius))
        api_arguments = {"token":token,"start":firstdt.synop(),"end":lastdt.synop(),"radius":st_radius,"units":units}
        api_request_url = get_base_api_request_url("timeseries")
        req = requests.get(api_request_url, params=api_arguments)
        data = req.json()
        db_object.add_observations(data)

    return(data or obsdb)

def check_db_radius_datetime(latitude,longitude,radius,firstdt,lastdt,db_object):
    # Stubbed because this is hard.
    # Proposed solution: 1) Return all stations within radius at firstdt. 2) Iterate over stations,
    # getting all observations and checking completeness within time window 3) Merge all station
    # observations into one object 4) As soon as any missing station or data is found, exit returning
    # False.
    return False

def get_max_gust(latitude,longitude, mgtime, timetpl, geotpl,db_object):
    # Returns the maximum wind gust speed at a location during a time window.
    # Accepts real latitude, longitude, and radius. 'time' is a TimeUtils object.
    # The timetpl is a tuple object containing time windows in hours. For example (1,2) would be a
    # one and two hour window. For now, no sorting, so order these increasing.
    # The geotpl is a tuple object containing radius windows. For example, (4,8) would be 4 and 8 mile
    # radii around the specified latitude and longitude. For now, no sorting, so order these increasing.
    # get_max_gust returns an m X n array of tuples, where m is the number of time windows and n is the
    # number of radius windows. The tuple returned for each is (time, weather station stid, maximum
    # gust).
    
    twindows = len(timetpl)
    gwindows = len(geotpl)

    # Get data for maximum radius and time window

    thi = TimeUtils(mgtime.datetime.datetime + timedelta(hours=timetpl[twindows-1]/2))
    tlo = TimeUtils(mgtime.datetime.datetime - timedelta(hours=timetpl[twindows-1]/2))
    wmobs = get_observations_by_radius_datetime(latitude,longitude,geotpl[gwindows-1],tlo,thi,db_object)

    # Return data object: time bins X radius bins X [stid, distance, datetime, max gust, count]
    womax = [[[None,None,None,0,0] for i in range(gwindows)] for j in range(twindows)]
        
    wtlst = []
    i = 0

    for tw in [-0.5,0.5]:
        for tm in timetpl:
            wtlst.append(mgtime.datetime.datetime + timedelta(hours=tm*tw))

    wtlst.sort(key=datetime.datetime.isoformat)

    if wmobs['SUMMARY']['NUMBER_OF_OBJECTS'] > 0 : 
        for wo in wmobs['STATION']:
            stid = wo['STID']
            strad = wo['DISTANCE']
            for wmevi in range(len(wo['OBSERVATIONS']['date_time'])):   # For each event in the time range
                for gi in range(gwindows):                              # For each distance bin   
                    for ti in range(twindows):                          # For each time bin
                        tevt = TimeUtils(wo['OBSERVATIONS']['date_time'][wmevi])   # Get event TimeUtils object
                        tdelta = abs(wtlst[ti] - tevt.datetime.datetime).seconds/3600 # Get evt-time bin diff (hrs)
                        if tdelta <= timetpl[ti]:                       # Is time difference in time bin?
                            if strad <= geotpl[gi]:                     # Is dist within dist bin?
                                if 'wind_gust_set_1' in wo['OBSERVATIONS'] and \
                                   wo['OBSERVATIONS']['wind_gust_set_1'][wmevi] != None:  # Check station monitors gusts
                                    womax[ti][gi][4] += 1       # Count per time/radius bin
                                    if wo['OBSERVATIONS']['wind_gust_set_1'][wmevi] >= womax[ti][gi][3]: # Largest
                                        womax[ti][gi][0] = stid
                                        womax[ti][gi][1] = strad
                                        womax[ti][gi][2] = wo['OBSERVATIONS']['date_time'][wmevi]
                                        womax[ti][gi][3] = wo['OBSERVATIONS']['wind_gust_set_1'][wmevi]

    return womax
    
class TimeUtils(object):

    # The TimeUtils class handles coversion between synoptic API (YYYYMMDDHHSS, UTC), synoptic data
    # (Zulu), and local tuple data ((y,m,d,h,m), Pacific). Data will be stored as a Zulu object.

    def __init__(self, timeobj):

        if (isinstance(timeobj,TimeUtils)):
            logging.warning("Time object is already a TimeUtils instance. Null operation.")
            self.datetime = timeobj.datetime
        else:
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
                    logging.error("Invalid time string format for " + timeobj)

    def __sub__(self,tut):
        # Subtraction operation returns datetime.timedelta object
        try: 
            dt = self.datetime.datetime - tut.datetime.datetime
        except TypeError:
            logging.error("Second operand must be TimeUtils object")
        return dt

    def synop(self):
        # synoptic string is YYYYMMDDHHMM
        ztpl = self.datetime.utctimetuple()
        synopstr = str(ztpl[0]) + str(ztpl[1]).zfill(2) + str(ztpl[2]).zfill(2) + \
            str(ztpl[3]).zfill(2) + str(ztpl[4]).zfill(2)
        return synopstr

    def randtime(start,end):
        dt1 = TimeUtils(start)
        dt2 = TimeUtils(end)
        delta = dt2 - dt1
        delsec = delta.days*24*60*60 + delta.seconds
        randsec = random.randrange(delsec)
        rt = dt1.datetime + datetime.timedelta(seconds=randsec)
        rantm = TimeUtils(rt)
        return rantm
    randtime = staticmethod(randtime)

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
        logging.info("Opening " + db_name)
        try:
            connection = sqlite3.connect(db_name)
        except Error as e:
            logging.error(e)
        self.connection = connection
        self.cursor = connection.cursor()
        self.db_name = db_name
        
    def create(db_name):
        if os.path.isfile(db_name):
            raise ValueError(db_name + " already exists. Use WeatherDB(db_name).")
        logging.info("Creating " + db_name)
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
            logging.debug("Station var: " + v + "   SQL Type: " + sqltype)
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
        logging.debug(sql)
        try:
            mydb.cursor.execute(sql)
        except Error as e:
            logging.error(e)

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
            logging.debug("Observation var: " + v + "   SQL Type: " + sqltype)
            comma = ', '
            if sqltype != 'REFERENCE' and v != 'date_time':
                sql = sql + v.lower() + '  ' + sqltype + comma
        
        sql = sql + ' volt_set_1 REAL, stid TEXT NOT NULL, PRIMARY KEY (stid, date_time), FOREIGN KEY (stid) REFERENCES station (stid) );'
        logging.debug(sql)
        try:
            mydb.cursor.execute(sql)
        except Error as e:
            logging.error(e)

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
                logging.error("Unrecognized station data structure")           
        for st in starr:
            sql = 'INSERT OR IGNORE INTO station('
            dbtuple = ()
            qm = ''
            for sd in st.keys():
                if sd not in ['OBSERVATIONS','QC','PERIOD_OF_RECORD','SENSOR_VARIABLES']:
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
                if data['SUMMARY']['NUMBER_OF_OBJECTS'] == 0 :
                    logging.warn("No observations found. Skipping.")
                    return
                else:
                    logging.error("Unrecognized station data structure")
                    raise
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
                    if var in db_schema:
                        val = (station['OBSERVATIONS'][okey][i],)
                        if i == 0:
                            sql = sql + var + ','
                            qm = qm + '?,'
                        obtuple = obtuple + val
                obtuple = obtuple + (stid,)
                obar.append(obtuple)
            qm.strip(',')
            sql = sql + 'stid) VALUES('+ qm + '?);'
            logging.debug(sql)
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
    mydb0.add_observations(radius_data)

    bt1 = TimeUtils('201609251534')
    bt2 = TimeUtils('201609251934')
    butte_data = get_observations_by_radius_datetime(38.801857,-122.817551,8.0,bt1,bt2,mydb0)
    bta = TimeUtils('201609241734')
    ttpl = (1,2)
    gtpl = (4,8)
#    butte_max = get_max_gust(38.801857,-122.817551,bta,ttpl,gtpl,mydb0)
    rt12a = TimeUtils.randtime(bt1,bt2)
    rt12b = TimeUtils.randtime(bt1,bt2)
    rt12c = TimeUtils.randtime(bt1,bt2)
    rt1X = TimeUtils.randtime(bt1,'201809010000')

    mydb0.close()
