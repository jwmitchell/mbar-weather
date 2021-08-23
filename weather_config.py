###
# Import configuration and make it globally available
#

import configparser
DEFAULT_CONFIG = 'weather.ini'

def init(weather_config_file):

    try:
        my_file = open(weather_config_file)
    except IOError:
        print ("Config file " + weather_config_file + " doesn't exist")

    config.read(weather_config_file)

config = configparser.ConfigParser()
