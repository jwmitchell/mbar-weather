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
            
if __name__ == '__main__':
    unittest.main()
