import urllib.request
import schedule
import time
import datetime
import os
import logging
from logging.handlers import TimedRotatingFileHandler

import json
import requests

import numpy as np
import xarray as xr

# [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1108)
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


default_path = '/home/notebook/weather/forecast'
forecast_api_url = f'http://10.0.2.7:18001'

# logger 설정
logger = logging.getLogger(name)
formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(funcName)s> %(message)s')
logger.setLevel(logging.DEBUG)

# logger에 file handler 추가
fileHandler = TimedRotatingFileHandler(filename=f'{default_path}/logs/forecast_wind.log', when='midnight', interval=1, encoding='utf-8')
fileHandler.setFormatter(formatter)
fileHandler.suffix = '%Y%m%d'
# fileHandler.setLevel(logging.ERROR)
logger.addHandler(fileHandler)

# logger에 stream handler 추가
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
streamHandler.setLevel(logging.DEBUG)
logger.addHandler(streamHandler)


# make url as request form
def download_url(base_url, list_vars, dims):
    '''
    base_url:
        NWW3, csv : "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NWW3_Global_Best.csv?"
        NWW3, nc  : "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NWW3_Global_Best.nc?"
        GFS, csv : 
        GFS, nc  : "https://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best.nc?" 
    
    list_vars:
        NWW3: subset of ['Tdir', 'Tper', 'Thgt', 'sdir', 'sper', 'shgt', 'wdir', 'wper', 'whgt'] 
        GFS:  subset of 
    
    dims: 
        NWW3: time, depth, latitude, longitude
        ex) %5B(2022-02-23T18:00:00Z):1:(2022-02-23T23:00:00Z)%5D%5B(0.0):1:(0.0)%5D%5B(-77.5):1:(77.5)%5D%5B(0.0):1:(359.5)%5D
        GFS:  time, latitude, longitude
        ex) %5B(2022-02-28T06:00:00Z):1:(2022-04-15T06:00:00Z)%5D%5B(-77.5):1:(77.5)%5D%5B(0.0):1:(359.5)%5D
        
    '''
    url = base_url
    for var in list_vars:
        subquery = var + dims
        url = url + subquery + ','
        # print(subquery)
    # print(url)
    logger.info(url)
    return url[:-1] # 맨 뒤에 , 빼고


def download_urlretrieve(base, grid_vars, dimensions, fn):    
    stime = time.time()    
    
    try:
        file_name, header = urllib.request.urlretrieve(download_url(base, grid_vars, dimensions), fn)    
        # urllib.request.urlretrieve(download_url(base, grid_vars, dimensions), fn)    
    except Exception as e:
        logger.info(f'download failed')
        logger.debug(e)
        
        return False
    else:
        logger.info(f'download success: {file_name}')
        logger.info(header)        
        timeit = time.time() - stime
        # print('download time: ',timeit,' s')
        logger.info(f'download time: {timeit} s')

        file_size = os.path.getsize(fn) 
        # print('file size: ',int(file_size/1024/1024),' Mbytes')
        logger.info(f'file size: {int(file_size/1024/1024)} Mbytes')
        
        return True

# inform API Server to update forecast data
def send_request_to_update(kind, filename):
    update_url = f'{forecast_api_url}/update/{kind}'
    # print(url)

    payload=json.dumps({'filename': filename})
    headers = {
      'Content-Type': 'application/json'
    }
    
    try:
        response = requests.request("POST", update_url, headers=headers, data=payload)
    except Exception as e:        
        # logger.info(response)
        logger.debug(e)
    else:
        logger.info("API server has response")
        logger.info(response.text)

        if response.text == f'{kind} data updated':
            remove_url = f'{forecast_api_url}/remove/{kind}'
            payload=json.dumps({'filename': filename})
            headers = {
            'Content-Type': 'application/json'
            }
            try:
                response = requests.request("POST", remove_url, headers=headers, data=payload)
            except Exception as e:        
                # logger.info(response)
                logger.debug(e)




def job_wind():
    # base url
    base = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/NCEP_Global_Best.nc?'
    
    # 시간 차원
    dateformat="%Y-%m-%dT%H:%M:%SZ"
    now = datetime.datetime.utcnow()
    days = 5
    end = now+datetime.timedelta(days=days)
    start = now.strftime(dateformat)
    stop = end.strftime(dateformat)
    
    # 공간 차원
    lat_min =  -90.0
    lat_max =  90.0
    lon_min = 0.0
    lon_max = 359.5
    
    grid_vars = ['ugrd10m', 'vgrd10m']
    
    dimensions = f'%5B({start}):1:({stop})%5D%5B({lat_min}):1:({lat_max})%5D%5B({lon_min}):1:({lon_max})%5D'
    
    dateformat2="%Y-%m-%d_%H" # for filename
    fn = f'{default_path}/data/noaa_gfs_{days}d_{now.strftime(dateformat2)}.nc'
    
    # print('wind data download from: ', start)    
    logger.info(f'start wind data download: {start} ~ {stop}')        
    
    if download_urlretrieve(base, grid_vars, dimensions, fn):
        # send request to update API Server
        send_request_to_update('wind', fn)

if name=="main":
    '''
    # wavewatchIII data가 12시(UTC) 경 업데이트됨 -> 12+1(buffer)+9(local) = 22시(UTC+9)
    schedule.every().day.at("21:30").do(job_wind)
    schedule.every().day.at("22:00").do(job_wave)
    schedule.every().day.at("23:00").do(job_current) # temporary
      
    while True:
        schedule.run_pending()
        time.sleep(1)

    --> cron-job schedule : Everyday 23:00 (UTC timezone)

    '''

    job_wind()