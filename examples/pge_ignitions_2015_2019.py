import sys
from datetime import datetime
import pytz
import logging
from openpyxl import load_workbook
import xlrd  # pip install
import weather_config
import weather_utils

logging.basicConfig(level=weather_config.config['Default']['LOG_LEVEL'])

xl_data = weather_config.config['PGE']['XL_DATA_FILE']
weather_db = weather_config.config['PGE']['WEATHER_DB']
xl_input_sheet = weather_config.config['PGE']['XL_INPUT_SHEET']
xl_output_sheet = weather_config.config['PGE']['XL_OUTPUT_SHEET']
ttpl_raw = weather_config.config['PGE']['TIME_WINDOWS']
gtpl_raw = weather_config.config['PGE']['DISTANCE_WINDOWS']

tlst = ttpl_raw.split(',')
ttpl = (int(tlst[0]),int(tlst[1]))
glst = gtpl_raw.split(',')
gtpl = (int(glst[0]),int(glst[1]))

try: 
    pgeigndb = weather_utils.WeatherDB(weather_db)   
except:
    pgeigndb = weather_utils.WeatherDB.create(weather_db)

logging.info('Opening workbook ' + xl_data)
wbk = load_workbook(filename=xl_data)

sht_all = wbk[xl_input_sheet]
sht_wind = wbk[xl_output_sheet]

frow = int(weather_config.config['PGE']['FIRST_ROW'])
lrow = int(weather_config.config['PGE']['LAST_ROW'])


for irow in range(frow,lrow+1):
    
    # Copy row

    srow = str(irow)
    logging.info("Processing line " + srow)
    for icol in range(len(sht_all[srow])):
        sht_wind[srow][icol].value = sht_all[srow][icol].value

    # Convert times to UTC for synoptic run. PG&E has multiple date/time formats
    # in their data set:
    # Case 1   B: Excel datetime    C: Excel datetime
    # Case 2   B: Excel date        C: Excel datetime
    # Case 3   B: Excel datetime    C: Excel time
    # Case 4   B: Excel date        C: Excel time
    # Case 5   B: Excel datetime    C: HH:MM
    # Case 6   B: DD/MM/YYYY        C: HH:MM  - Adding this for additional fires

    tcelld = "B"+ srow
    tcellt = "C"+ srow
    tmxld = sht_wind[tcelld].value
    tmxlt = sht_wind[tcelld].value

    is_xl_format = False
    tmxl = 0

    try: 
        if type(tmxld) is str:
            if '/' in tmxld:
                dd = xcelld.split('/')
                tt = xcellt.split(':')
                tmpydt = datetime.datetime(int(dd[0]),int(dd[1]),int(dd[2]),\
                                           int(tt[0]),int(tt[1]))
        if isinstance(tmxld,datetime):
            tmpydt = tmxld
        elif float(tmxld) > 40000.00 and float(tmxld) < 50000.00:
            is_xl_format = True
            if not float(tmxld).is_integer():
                tmxl = float(tmxld) 
            elif float(tmxlt) > 40000.0 and float(tmxlt) < 50000.0:
                tmxl = float(tmxlt)
            elif float(tmxlt) < 1.0:
                tmxl = float(tmxld) + float(tmxlt)
            else:
                raise TypeError("Unrecognized time value pair B:" + str(tmxld) + " C:" + str(tmxlt))
            tmpydt = datetime(*xlrd.xldate_as_tuple(tmxl,0))
    except TypeError:
        logging.warning("Exiting on error. Saving workbook " + xl_data)
        wbk.save(xl_data)
        raise

    tmlocal = pytz.timezone("America/Los_Angeles").localize(tmpydt)
    tmutc = tmlocal.astimezone(pytz.utc)

    zigtime = weather_utils.TimeUtils(tmutc)
    lat = sht_wind["D"+srow].value
    lon = sht_wind["E"+srow].value

    # Get list of max wind data based on time and space windows
    # Output is m x n list of  [station_id,time,distance,maximum gust,count]
    
    try:
        max_gusts = weather_utils.get_max_gust(lat,lon,zigtime,ttpl,gtpl,pgeigndb)
    except:
        logging.warning("Exiting on error. Saving workbook " + xl_data)
        wbk.save(xl_data)
        raise
    
    # Write output to cells
    
    icell = int(weather_config.config['PGE']['FREE_CELL'])
    for it in max_gusts:
        for ig in it:
            for val in ig:
                sht_wind[srow][icell].value = val
                icell += 1
    
logging.info("Complete. Saving workbook " + xl_data)
wbk.save(xl_data)
