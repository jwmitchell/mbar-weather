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

class WeatherDBTest(unittest.TestCase):

    @classmethod
    def setUpClass(WeatherDBTest):
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
        mydb = None
        with self.assertRaises(FileExistsError):
            mydb = weather_utils.WeatherDB('bogus_db')

    def test_db_already_exists(self):
        db_name = 'test/test_weather_data.db'
        with self.assertRaises(ValueError):
            mydb = weather_utils.WeatherDB.create(db_name)
    
    def test_open(self):
        db_name = 'test/test_weather_data.db'
        mydb = weather_utils.WeatherDB(db_name)
        sql = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='station'";
        mydb.cursor.execute(sql)
        self.assertTrue(mydb.cursor.fetchone())
        mydb.close()
        
    def test_add_stations(self):
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
        mydb = self._connection
        stid = 'BOGUS'
        stdat = mydb.get_station(stid)
        self.assertEqual(stdat,{}) 

    def test_existing_station(self):
        mydb = self._connection
        stid = 'PG133'
        tdat = mydb.get_station(stid)
        self.assertEqual(stid,tdat['STID'])    

    def test_close(self):
        db_name = 'test/test_weather_data.db'
        mydb = weather_utils.WeatherDB(db_name)
        mydb.close()
        self.assertTrue(mydb.cursor == None)
        
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

    def tearDown(self):
        try:
            os.remove('test/test_weather_data.db')
        except:
            pass
        
        
if __name__ == '__main__':
    unittest.main()
