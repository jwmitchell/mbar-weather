###
##  Test suite for weather_utils.py

import weather_utils
import unittest
import os

# weather_utils.create_weather_db('test_weather_data.db')

class DBCreateTestCase(unittest.TestCase):

    def setUp(self):
        try:
            os.remove('test_weather_data.db')
        except:
            pass
            
    def test_create_db(self):
        weather_utils.create_weather_db('test_weather_data.db')

    def test_db_already_exists(self):
        weather_utils.create_weather_db('test_weather_data.db')
        with self.assertRaises(RuntimeError):
            weather_utils.create_weather_db('test_weather_data.db')

    def tearDown(self):
        try:
            os.remove('test_weather_data.db')
        except:
            pass


class BaseAPIRequestTestCase(unittest.TestCase):

    def test_timeseries(self):
        url = weather_utils.get_base_api_request_url('timeseries')
        self.assertEqual(url,'https://api.synopticdata.com/v2/stations/timeseries')

    def test_invalid_type(self):
        with self.assertRaises(ValueError):
            url = weather_utils.get_base_api_request_url('bogus')
    
if __name__ == '__main__':
    unittest.main()
