###
##  Test suite for weather_utils.py

import weather_utils
import unittest
import os
import sqlite3

class BaseAPIRequestTestCase(unittest.TestCase):

    def test_timeseries(self):
        url = weather_utils.get_base_api_request_url('timeseries')
        self.assertEqual(url,'https://api.synopticdata.com/v2/stations/timeseries')

    def test_invalid_type(self):
        with self.assertRaises(ValueError):
            url = weather_utils.get_base_api_request_url('bogus')

class Python2SQLTestCase(unittest.TestCase):

    def test_string_type(self):
        python_object = "string"
        sqltype = weather_utils.python_to_sql(python_object)
        self.assertEqual(sqltype,'TEXT')

    def test_int_type(self):
        python_object = 4
        sqltype = weather_utils.python_to_sql(python_object)
        self.assertEqual(sqltype,'INTEGER')

    def test_real_type(self):
        python_object = 3.14
        sqltype = weather_utils.python_to_sql(python_object)
        self.assertEqual(sqltype,'REAL')

    def test_dict_type(self):
        python_object = {'a':1, 'b':'fred'}
        sqltype = weather_utils.python_to_sql(python_object)
        self.assertEqual(sqltype,'REFERENCE')

    def test_other_type(self):
        python_object = [1, 2, 3]
        with self.assertRaises(TypeError):
            sqltype = weather_utils.python_to_sql(python_object)
            print ("sqltype " + sqltype)

class TimeUtilsTest(unittest.TestCase):

    def test_datetime(self):

        dt1 = '2019-10-10T03:40:00Z'   # Zulu time format
        dt2 = (2012,12,12,13,3)        # Time tuple format (Should be Pacific)
        dt3 = '201910092311'           # Synoptic format
        dta = (2012,88,0,0,0)          # Nonsense time
        dtb = 'foobiar'                # Random string
        
        tudt1 = weather_utils.TimeUtils(dt1)
        tudt2 = weather_utils.TimeUtils(dt2)
        tudt3 = weather_utils.TimeUtils(dt3)

        self.assertEqual(tudt1.datetime.datetime.month,10)
        self.assertEqual(tudt2.datetime.datetime.year,2012)
        self.assertEqual(tudt3.datetime.datetime.minute,11)

        with self.assertRaises(ValueError):
            tudta = weather_utils.TimeUtils(dta)
        with self.assertRaises(ValueError):
            tudtb = weather_utils.TimeUtils(dtb)

    def test_synop(self):

        dt1 = '2019-10-10T03:40:00Z'
        tudt1 = weather_utils.TimeUtils(dt1)
        sydt1 = tudt1.synop()
        self.assertEqual(sydt1,'201910100340')
        
class WeatherDBTest(unittest.TestCase):

    @classmethod
    def setUpClass(WeatherDBTest):
        print('WeatherDBTest - SetUp')
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass
        db_name = 'test/test_weather_data.db'
        radius_data = weather_utils.get_example_radius_dataset()
        WeatherDBTest._connection = weather_utils.WeatherDB.create(db_name)
        WeatherDBTest._connection.add_station(radius_data)
        try:
            WeatherDBTest.test_data = eval(open('test/test_novato_1.dat', 'r').read())
        except Error as e:
            print(e)
    
    def test_create_db(self):
        print('WeatherDBTest - test_create_db')
        try:
            os.remove('test/test_weather_data_2.db')
        except:
            pass
        db_name = 'test/test_weather_data_2.db'
        mydb = weather_utils.WeatherDB.create(db_name)
        sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='station'";
        mydb.cursor.execute(sql)
        self.assertTrue(mydb.cursor.fetchone())
        mydb.close()
        os.remove('test/test_weather_data_2.db')
        
    def test_db_unavailable(self):
        print('WeatherDBTest - test_db_unavailable')
        mydb = None
        with self.assertRaises(FileExistsError):
            mydb = weather_utils.WeatherDB('bogus_db')

    def test_db_already_exists(self):
        print('WeatherDBTest - test_db_already_exists')
        db_name = 'test/test_weather_data.db'
        with self.assertRaises(ValueError):
            mydb = weather_utils.WeatherDB.create(db_name)
    
    def test_open(self):
        print('WeatherDBTest - test_open')
        db_name = 'test/test_weather_data.db'
        mydb = weather_utils.WeatherDB(db_name)
        sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='station'";
        mydb.cursor.execute(sql)
        self.assertTrue(mydb.cursor.fetchone())
        mydb.close()
        
    def test_add_stations(self):
        print('WeatherDBTest - test_add_stations')
        try:
            os.remove('test/test_weather_data_2.db')
        except:
            pass
        db_name = "test/test_weather_data_2.db"
        mydb = weather_utils.WeatherDB.create(db_name)
        mydb.add_station(self.test_data)
        mydb.cursor.execute('SELECT stid FROM station')
        stations = mydb.cursor.fetchall()
        st3, = stations[3]
        self.assertEqual(st3,'PG133')
        mydb.close()
        os.remove('test/test_weather_data_2.db')

    def test_invalid_station(self):
        print('WeatherDBTest - test_invalid_station')
        mydb = self._connection
        stid = 'BOGUS'
        stdat = mydb.get_station(stid)
        self.assertEqual(stdat,{}) 

    def test_existing_station(self):
        print('WeatherDBTest - test_existing_station')
        mydb = self._connection
        stid = 'PG133'
        tdat = mydb.get_station(stid)
        self.assertEqual(stid,tdat['STID'])

    def test_add_observations(self):
        print('WeatherDBTest - test_add_observations')
        mydb = self._connection
        mydb.add_observations(self.test_data)
        mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = mydb.cursor.fetchone()
        (count,) = countpl
        self.assertEqual(count,154)

    def test_unique_observations(self):
        print('WeatherDBTest - test_unique_observations')
        mydb = self._connection
        mydb.add_observations(self.test_data)
        mydb.add_observations(self.test_data)
        mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = mydb.cursor.fetchone()
        (count,) = countpl
        self.assertEqual(count,154)

    def test_no_observations(self):
        print('WeatherDBTest - test_no_observations')
        mydb = self._connection
        bogus_data = None
        with self.assertRaises(ValueError):
            mydb.add_observations(bogus_data)

    def test_bad_observations(self):
        print('WeatherDBTest - test_bad_observations')
        mydb = self._connection
        bogus_data = {'first': ['alpha','beta','gamma'],'second':[1,2,3]}
        with self.assertRaises(KeyError):
            mydb.add_observations(bogus_data)

    def test_close(self):
        db_name = 'test/test_weather_data.db'
        mydb = weather_utils.WeatherDB(db_name)
        mydb.close()
        self.assertTrue(mydb.cursor == None)

    def test_get_observations(self):
        mydb = self._connection
        stid = 'PG133'
        ostart = '2019-10-09T23:11:00Z'
        ofinish = '2019-10-10T03:11:00Z'
        obs = mydb.get_observations(stid,ostart,ofinish)
        self.assertEqual(obs[0]['DATE_TIME'],'2019-10-09T23:20:00Z')

    def test_no_observations(self):
        mydb = self._connection
        stid = 'PG133'
        ostart = '2019-10-12T23:11:00Z'
        ofinish = '2019-10-13T03:11:00Z'
        obs = mydb.get_observations(stid,ostart,ofinish)
        self.assertEqual(obs,[])
        
    @classmethod
    def tearDownClass(WeatherDBTest):
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass

class TestGetStationBySTIDTestCase(unittest.TestCase):

    def setUp(self):
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass
        try:
            test_data = eval(open('test/test_novato_1.dat', 'r').read())
        except Error as e:
            print(e)
        db_name = 'test/test_weather_data.db'
        self.mydb = weather_utils.WeatherDB.create(db_name)
        radius_data = weather_utils.get_example_radius_dataset()
        self.mydb.add_station(radius_data)

    def test_get_existing_station(self):
        stid = 'PG133'
        tdat = weather_utils.get_station_by_stid(stid,self.mydb)
        self.assertEqual(stid,tdat['STID'])

    def test_fetch_new_station(self):
        stid = 'PG130'
        tdat = weather_utils.get_station_by_stid(stid,self.mydb)
        self.assertEqual(stid,tdat['STID'])
                    
    def test_no_station(self):
        stid = 'NOSTATION'
        with self.assertRaises(ValueError):
            stdat = weather_utils.get_station_by_stid(stid,self.mydb)

    def test_get_obs_by_stid_datetime(self):
        stid = 'PG133'
        dt1 = '2019-10-11T23:11:00Z'
        dt2 = '2019-10-12T01:11:00Z'
        self.mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = self.mydb.cursor.fetchone()
        # Baseline for number of observations in db
        (count0,) = countpl
        
        obs = weather_utils.get_observations_by_stid_datetime(stid,dt1,dt2,self.mydb)
        self.assertEqual(obs[0]['DATE_TIME'],'2019-10-11T23:20:00Z')
        self.mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = self.mydb.cursor.fetchone()
        (count1,) = countpl
        self.assertEqual(count1-count0,12)

        # Should rerun with same results
        obs = weather_utils.get_observations_by_stid_datetime(stid,dt1,dt2,self.mydb)
        self.assertEqual(obs[0]['DATE_TIME'],'2019-10-11T23:20:00Z')
        self.mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = self.mydb.cursor.fetchone()
        (count2,) = countpl
        self.assertEqual(count1,count2)

        # Should return additional results if window expanded
        dt3 = '2019-10-12T01:41:00Z'
        obs = weather_utils.get_observations_by_stid_datetime(stid,dt1,dt3,self.mydb)
        self.assertEqual(obs[11]['DATE_TIME'],'2019-10-12T01:10:00Z')
        self.mydb.cursor.execute('SELECT count(*) FROM observations;')
        countpl = self.mydb.cursor.fetchone()
        (count3,) = countpl
        self.assertEqual(count3-count2,3)        

    def tearDown(self):
        try:
            close('test/test_novato_1.dat')
            os.remove('test/test_weather_data.db')
        except:
            pass
        

class GetMaxGustTestCase(unittest.TestCase):

    def setUp(self):
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass

        db_name = 'test/test_weather_data.db'
        self.mydb = weather_utils.WeatherDB.create(db_name)

    def test_max_gust(self):

        # I1004 test
        ttpl = (1,2)
        gtpl = (4,8)
        lat = 38.801857
        lon = -122.817551
        tm = weather_utils.TimeUtils('201609251734')
        mg = weather_utils.get_max_gust(lat,lon,tm,ttpl,gtpl,self.mydb)
        self.assertEqual(mg[0][1][0],'HWKC1')
        self.assertEqual(mg[1][1][4]/mg[0][1][4],2)  # Should be double in tm window

    def tearDown(self):
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass
        
if __name__ == '__main__':
    unittest.main()
