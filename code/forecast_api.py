import os
import sys
import yaml
import math
import json
import logging
import traceback
import datetime
import numpy as np
import xarray as xr
import pandas as pd

from collections import defaultdict
from os.path import exists as file_exists
from flask import Flask, request, jsonify
from logging.handlers import TimedRotatingFileHandler

# default path
default_path = '/home/notebook/weather/forecast'
api_port = 18001

filename_dict = {
    'wind': 'noaa_gfs_4d',
    'swell': 'noaa_nww3_s_3d',
    'wwave': 'noaa_nww3_w_3d',
    'twave': 'noaa_nww3_T_3d',
    'current': 'hycom_current'
}

# logger 설정
logger = logging.getLogger(name)
formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(funcName)s> %(message)s')
logger.setLevel(logging.DEBUG)

# logger에 file handler 추가
fileHandler = TimedRotatingFileHandler(filename=f'{default_path}/logs/forecast_api.log', when='midnight', backupCount=10, interval=1, encoding='utf-8')
fileHandler.setFormatter(formatter)
fileHandler.suffix = '%Y%m%d'
logger.addHandler(fileHandler)

# logger에 stream handler 추가
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
streamHandler.setLevel(logging.DEBUG)
logger.addHandler(streamHandler)

# In[3]:


def convert_degree_to_radian(df, degree_columns=None):
    
    if degree_columns == None:
        degree_columns = {
            # direction value(key) : height value(value)
            'Tdir' : 'Thgt', 
            'sdir' : 'shgt',
            'wdir' : 'whgt',
        }
            
    for column in list(df.columns):
        if column in list(degree_columns.keys()):
            df[f'{column}_sin'] = df[column].apply(lambda x: math.sin(math.radians(x)))
            df[f'{column}_cos'] = df[column].apply(lambda x: math.cos(math.radians(x)))

    return df


def datetime_to_timestamp(date_str):
    """Convert datetime to timestamp"""
    return np.int64(pd.to_datetime(date_str).timestamp() * 1000)


def convert_unixtime(unixtime, **kwargs):
    """Convert unixtime to datetime"""
    if len(str(np.int64(unixtime))) == 13: # ms
        unixtime = np.int64(unixtime)/1000

    dt_format = kwargs.get('format', '%Y-%m-%d %H:%M:%S')
    return datetime.datetime.utcfromtimestamp(unixtime).strftime(dt_format)


def add_timeindex(df, timestamp='ds_timestamp'):
    """Convert Timestamp to DateTimeIndex"""
    df[timestamp] = df[timestamp].astype(float)
    df['ds_timeindex'] = pd.to_datetime(
        df[timestamp].apply(lambda row: convert_unixtime(row))
        )
    df[timestamp] = df[timestamp].astype(np.int64)
    return df


def add_timestamp(df):
    datetimetotimestamp = lambda x: int(x.timestamp() * 1000)  # units : ms
    vf_dttots = np.vectorize(datetimetotimestamp)
    df['ds_timestamp'] = vf_dttots(list(df.index))
    df['ds_timestamp'] = df['ds_timestamp'].astype(int)
    return df

def weather_preprocessing(df_weather, wtype):
    df_weather['ds_timeindex'] = pd.to_datetime(df_weather['time'])
    df_weather = df_weather.set_index('ds_timeindex')
    df_weather = add_timestamp(df_weather)
    
    if wtype == 'wave':

        columns = [
            'ds_timestamp',
            'latitude', 'longitude', 
            'Tper', 'Tdir', 'Thgt',
            'sper', 'sdir', 'shgt', 
            'wper', 'wdir', 'whgt'
        ]
        df_weather = df_weather.loc[:, columns]
        df_weather = convert_degree_to_radian(df_weather)
    
    elif wtype == 'wind':
        columns = [
            'ds_timestamp',
            'latitude', 'longitude', 
            'ugrd10m', 'vgrd10m'
        ]
        df_weather = df_weather.loc[:, columns]

    return df_weather

def near_coordinates(latitude, longitude):
    
    def _previous_coordinate(coord_min, coord_max, coord_gap, coord):
        coord_range = np.arange(start=coord_min, stop=coord_max, step=coord_gap)
        coord_bin = np.arange(start=coord_min - coord_gap, stop=coord_max, step=coord_gap)
        coord_previous = coord_bin[np.digitize(coord, coord_range)]
        return coord_previous

    config = {
        'latitude_min': -90.0, 
        'latitude_max': 90.0,
        'longitude_min': 0.0,  # -180.0,
        'longitude_max': 359.5,  # 180.0
        'grid_gap': 0.5
    }

    latitude_previous = _previous_coordinate(config['latitude_min'],
                                             config['latitude_max'],
                                             config['grid_gap'],
                                             latitude)
    latitude_next = latitude_previous + config['grid_gap']

    longitude_previous = _previous_coordinate(config['longitude_min'],
                                              config['longitude_max'],
                                              config['grid_gap'],
                                              longitude)
    longitude_next = longitude_previous + config['grid_gap']

    coordinates_list = [(lat, lon)
                        for lat in [latitude_previous, latitude_next]
                        for lon in [longitude_previous, longitude_next]]
    # sort list
    return coordinates_list

# for NOAA .nc files
def near_coordinates_noaa(latitude, longitude):
    
    def _previous_coordinate(coord_min, coord_max, coord_gap, coord):
        coord_range = np.arange(start=coord_min, stop=coord_max, step=coord_gap)
        coord_bin = np.arange(start=coord_min - coord_gap, stop=coord_max, step=coord_gap)
        coord_previous = coord_bin[np.digitize(coord, coord_range)]
        return coord_previous

    config = {
        'latitude_min': -90.0, 
        'latitude_max': 90.0,
        'longitude_min': 0.0,  # -180.0,
        'longitude_max': 359.5,  # 180.0
        'grid_gap': 0.5
    }

    latitude_previous = _previous_coordinate(config['latitude_min'],
                                             config['latitude_max'],
                                             config['grid_gap'],
                                             latitude)
    latitude_next = latitude_previous + config['grid_gap']

    longitude_previous = _previous_coordinate(config['longitude_min'],
                                              config['longitude_max'],
                                              config['grid_gap'],
                                              longitude)
    longitude_next = longitude_previous + config['grid_gap']

    lats = [latitude_previous, latitude_next]
    lons = [longitude_previous, longitude_next]
    
    return lats, lons

def near_coordinates_hycom(hycom_lat, hycom_lon, latitude, longitude):    
    lats = [min(hycom_lat, key=lambda x:abs(x-latitude)), max(hycom_lat, key=lambda x:abs(x-latitude))]
    lons = [min(hycom_lon, key=lambda x:abs(x-longitude)), max(hycom_lon, key=lambda x:abs(x-longitude))]
    return lats, lons

def near_hour_time(ds_timeindex, **kwargs):
    time_gap = kwargs.get('time_gap', 3)
    
    previous_time = pd.to_datetime(ds_timeindex)

    date_hour = previous_time.hour
    nearest_hour = date_hour - (date_hour % time_gap)

    previous_time = f"{previous_time.strftime('%Y-%m-%d')} {nearest_hour}:00"
    previous_time = pd.to_datetime(previous_time)

    next_time = previous_time + datetime.timedelta(hours=time_gap)
    
    ptime = datetime_to_timestamp(previous_time.round('H').strftime('%Y-%m-%d %H:00:00'))
    ntime = datetime_to_timestamp(next_time.round('H').strftime('%Y-%m-%d %H:00:00'))
    return ptime, ntime

def near_hour_time_str(ds_timeindex, **kwargs):
    time_gap = kwargs.get('time_gap', 3)
    
    previous_time = pd.to_datetime(ds_timeindex)

    date_hour = previous_time.hour
    nearest_hour = date_hour - (date_hour % time_gap)

    previous_time = f"{previous_time.strftime('%Y-%m-%d')} {nearest_hour}:00"
    previous_time = pd.to_datetime(previous_time)

    next_time = previous_time + datetime.timedelta(hours=time_gap)
    
    ptime = previous_time.round('H').strftime('%Y-%m-%d %H:00:00')
    ntime = next_time.round('H').strftime('%Y-%m-%d %H:00:00')
    return [ptime, ntime]

def interpolation(value_list, index_list, column_list):
    """ linear interpolation """
    interpolated = pd.DataFrame(
        value_list,
        index=index_list,
        columns=column_list
    ).interpolate(method='linear')
    return interpolated.iloc[1].values    

def euclidean_distance(lat1, lon1, lat2, lon2):
    distance = math.sqrt(math.pow(lat1-lat2, 2) + math.pow(lon1-lon2, 2))
    if float(distance) == 0.0:
        distance = pow(0.1, 10)
    return distance

def idw(df_weather, ds_timestamp, latitude, longitude, near_time, near_coord, degree_columns=None):
    
    func_name = sys._getframe().f_code.co_name
    
    try:
    
        df_nan = pd.DataFrame()
        df_nan['ds_timestamp'] = [ds_timestamp]
        df_nan['latitude'] = [latitude]
        df_nan['longitude'] = [longitude]

        if degree_columns == None:
            degree_columns = {
                # direction value(key) : height value(value)
                'Tdir' : 'Thgt', 
                'sdir' : 'shgt',
                'wdir' : 'whgt',
            }


        t1, t2 = near_time
        p1, p2, p3, p4 = near_coord


        t1_point = pd.DataFrame()
        t2_point = pd.DataFrame()

        is_t1 = df_weather['ds_timestamp'] == t1
        is_t2 = df_weather['ds_timestamp'] == t2

        is_p1_lat = df_weather['latitude'] == p1[0]
        is_p2_lat = df_weather['latitude'] == p2[0]
        is_p3_lat = df_weather['latitude'] == p3[0]
        is_p4_lat = df_weather['latitude'] == p4[0]

        is_p1_lon = df_weather['longitude'] == p1[1]
        is_p2_lon = df_weather['longitude'] == p2[1]
        is_p3_lon = df_weather['longitude'] == p3[1]
        is_p4_lon = df_weather['longitude'] == p4[1]

        t1_point = pd.concat([t1_point, df_weather[is_t1 & is_p1_lat & is_p1_lon]], axis=0, ignore_index=True)
        t1_point = pd.concat([t1_point, df_weather[is_t1 & is_p2_lat & is_p2_lon]], axis=0, ignore_index=True)
        t1_point = pd.concat([t1_point, df_weather[is_t1 & is_p3_lat & is_p3_lon]], axis=0, ignore_index=True)
        t1_point = pd.concat([t1_point, df_weather[is_t1 & is_p4_lat & is_p4_lon]], axis=0, ignore_index=True)
        t1_point = t1_point.sort_values(by=['ds_timestamp', 'latitude', 'longitude'])

        t2_point = pd.concat([t2_point, df_weather[is_t2 & is_p1_lat & is_p1_lon]], axis=0, ignore_index=True)
        t2_point = pd.concat([t2_point, df_weather[is_t2 & is_p2_lat & is_p2_lon]], axis=0, ignore_index=True)
        t2_point = pd.concat([t2_point, df_weather[is_t2 & is_p3_lat & is_p3_lon]], axis=0, ignore_index=True)
        t2_point = pd.concat([t2_point, df_weather[is_t2 & is_p4_lat & is_p4_lon]], axis=0, ignore_index=True)
        t2_point = t2_point.sort_values(by=['ds_timestamp', 'latitude', 'longitude'])

        if t1_point.empty and t2_point.empty:
                print(f"[{ds_timestamp}][{latitude}][{longitude}] t1_point.empty and t2_point.empty")
                return pd.Series(df_nan.values[0], index=list(df_nan.columns))


        weather_columns = [
            ele for ele in df_weather.columns 
            if ele not in ['ds_timestamp', 'latitude', 'longitude']
        ]


        for column in weather_columns:
            if ('sin' not in column) and ('cos' not in column):
                df_nan[column] = [None]
                
        # print("before timestamp interpolation")
        # timestamp linear interpolation =========================================================================
        time_interpolated_dict = {}
        time_interpolated_dict['latitude'] = t1_point['latitude'].values
        time_interpolated_dict['longitude'] = t1_point['longitude'].values

        for col in weather_columns:
            time_interpolated_dict[col] = interpolation(
                value_list=[t1_point[col].values, [], t2_point[col].values], 
                index_list=[t1, ds_timestamp, t2],
                column_list=near_coord
                )

        # dfw_itp: df_weather_interpolated
        dfw_itp = pd.DataFrame(time_interpolated_dict)

        # wave height : zero to pow(0.1, 10)
        try:
            wave_height_columns = list(degree_columns.values())
            dfw_itp[wave_height_columns] = dfw_itp[wave_height_columns].replace({0.0: pow(0.1, 10)})
        except:
            pass

        # print("before idw interpolation")
        # inverse distance weighting interpolation =================================================================
        df_result = pd.DataFrame()
        df_result['ds_timestamp'] = [ds_timestamp]
        df_result['latitude'] = [latitude]
        df_result['longitude'] = [longitude]

        # calculate idw distance
        dfw_itp['distance'] = dfw_itp.apply(
            lambda x : euclidean_distance(latitude, longitude, x['latitude'], x['longitude']), axis=1) 

        # calculate idw weight
        power_parameter = 1
        dfw_itp['weight'] = 1.0 / dfw_itp['distance'].pow(power_parameter)

        suminf = dfw_itp['weight'].sum()
        for col in weather_columns:

            # degree variables
            if col in degree_columns.keys():
                # weight * sin/cos * height
                dfw_itp[f'{col}_sin'] = dfw_itp['weight'] * dfw_itp[f"{col}_sin"] * dfw_itp[f'{degree_columns[col]}']
                dfw_itp[f'{col}_cos'] = dfw_itp['weight'] * dfw_itp[f"{col}_cos"] * dfw_itp[f'{degree_columns[col]}']

                sumsup_sin = dfw_itp[f'{col}_sin'].sum() / suminf
                sumsup_cos = dfw_itp[f'{col}_cos'].sum() / suminf

                df_result[col] = [ math.degrees(math.atan2(sumsup_sin, sumsup_cos)) ]

            # skip unnecessary variables
            elif ('sin' in col) or ('cos' in col):
                continue

            # general variables
            else:
                sumsup = (dfw_itp['weight'] * dfw_itp[col]).sum()
                df_result[col] = [sumsup / suminf]


        # degree scale : 0 ~ 360
        for col in weather_columns:
            if col in degree_columns.keys():
                df_result[col] = df_result[col].apply(lambda x: float(x) + 360.0 if float(x) < 0 else float(x))

        return pd.Series(df_result.values[0], index=list(df_result.columns))
    
    except Exception as exp:
        print(f"[{func_name}] {exp}")
        return pd.Series(df_nan.values[0], index=list(df_nan.columns))


# nc based interpolation - for noaa wind, wave
def idw_nc(nc, ds_timeindex, latitude, longitude, time_gap=3):
    """
        < input parameter>
        nc: noaa gfs .nc file or noaa ww3 .nc file
        ds_timeindex: target, ex) "2022-07-23 00:30:00"
        latitude, lontitude: target, -90 ~ 90 / -180 ~ 180
        time_gap: forecast data period - gfs: 3 hour, ww3: 1 hour
    """
    near_time_str = near_hour_time_str(ds_timeindex, time_gap=time_gap)    
    near_lats, near_lons = near_coordinates_noaa(latitude, longitude)    
    
    df_near8 = convert_degree_to_radian(nc.sel(time=near_time_str, latitude=near_lats, longitude=near_lons).to_dataframe().reset_index())               

    df_near8['ds_timestamp'] = df_near8['time'].astype(np.int64)/1000000
    
    # unit conversion for swell and windwave
    # The unit of total wave period is already 'second'.
    if 'wper' in df_near8.columns:
        df_near8['wper'] = df_near8['wper'].dt.total_seconds()
    if 'sper' in df_near8.columns:
        df_near8['sper'] = df_near8['sper'].dt.total_seconds()
#     if 'tper' in df_near8.columns:
#         df_near8['Tper'] = df_near8['Tper'].dt.total_seconds()
        
    # return df_near8

    timestamp = datetime_to_timestamp(ds_timeindex)
    near_time = near_hour_time(ds_timeindex, time_gap=time_gap)        # unix timestamp
    near_coord = near_coordinates(latitude, longitude)
        
    return idw(df_near8.drop(['time'], axis=1), timestamp, latitude, longitude, near_time, near_coord, degree_columns=None)

# Actions(Internal)
# load forecast data
def load_swell(filename):
    global nww3_s
    # check if the file exist or not
    if file_exists(filename):
        nww3_s = xr.open_dataset(filename)
        logger.info(f'swell forecast data is updated with {filename}')
    else:
        logger.info(f'cannot find {filename}')

def load_wwave(filename):
    global nww3_w
    # check if the file exist or not
    if file_exists(filename):
        nww3_w = xr.open_dataset(filename)
        logger.info(f'wind wave forecast data is updated with {filename}')
    else:
        logger.info(f'cannot find {filename}')

def load_twave(filename):
    global nww3_T
    # check if the file exist or not
    if file_exists(filename):
        nww3_T = xr.open_dataset(filename)
        logger.info(f'total wave forecast data is updated with {filename}')
    else:
        logger.info(f'cannot find {filename}')

def load_wind(filename):
    global gfs
    # check if the file exist or not
    if file_exists(filename):
        gfs = xr.open_dataset(filename)
        logger.info(f'wind forecast data is updated with {filename}')
    else:
        logger.info(f'cannot find {filename}')

def load_current(filename):
    global hycom
    # check if the file exist or not
    if file_exists(filename):
        hycom = xr.open_dataset(filename)

# 2025-10-21 edited by shipman98
# time3 variable is not in the dataset.
# edit time3-->time 
#
# if exist time3 variable rename time
# not exitst --> use time
        print(hycom) 
        print(hycom.variables)
        print(hycom.dims)
#       hycom = hycom.rename({'time3': 'time','time3_run': 'time_run'})

        rename_dict = {}
        if 'time3' in hycom.variables:
            rename_dict['time3'] = 'time'
        if 'time3_run' in hycom.variables:
            rename_dict['time3_run'] = 'time_run'

        if rename_dict:
            hycom = hycom.rename(rename_dict)

        logger.info(f'current forecast data is updated with {filename}')
    else:
        logger.info(f'cannot find {filename}')        

def get_latest_files():
    # 최신 환경 데이터 조회
    
    filename_list_dict = defaultdict(list)

    for filename in os.listdir(f"{default_path}/data"):
        for dtype, name in filename_dict.items():
            if name in filename:
                filename_list_dict[dtype].append(f"{default_path}/data/{filename}")
                filename_list_dict[dtype] = [
                    list(reversed(filename_list_dict[dtype]))[0]
                ]
                
    return filename_list_dict

def get_file_list():
    # 최신 환경 데이터 조회
    
    filename_list_dict = defaultdict(list)

    for filename in os.listdir(f"{default_path}/data"):
        for dtype, name in filename_dict.items():
            if name in filename:
                filename_list_dict[dtype].append(f"{default_path}/data/{filename}")
                filename_list_dict[dtype] = list(reversed(filename_list_dict[dtype]))
                
    return filename_list_dict

# NOAA GFS wind, WW3 wave
def get_noaa_data(noaasource, pt, time_gap):
    return idw_nc(noaasource, pt['time'], pt['lat'], pt['lon'], time_gap).to_dict()

# HYCOM current
def get_hycom_data(hycomsource, pt):
    df = hycomsource.interp(time=pt['time'], lat=pt['lat'], lon=pt['lon']).to_dataframe()
    r = df.to_dict(orient='records')[0]
    r['time'] = r['time'].strftime("%Y-%m-%d %H:%M:%S")
    return r

# added at v1.2
def check_coverage_end(time_coverage_end, pt):
    # pt = {"time": "2022-10-23 21:00:00", "lon": 155.5, "lat": 12}
    pt["time_requested"] = pt["time"]
    rdt = datetime.datetime.strptime(pt["time"], '%Y-%m-%d %H:%M:%S')                 
    # cdt = datetime.datetime.strptime(time_coverage_end, '%Y-%m-%dT%H:%M:%SZ')
    cdt = time_coverage_end # 마지막 시간대에는 환경데이터가 안 들어 있음
    
    if rdt > cdt:
        # print("beyond: ", time_coverage_end)
        logger.info(f"beyond: {time_coverage_end}")
        # print(gfs.attrs['time_coverage_end'])
        pt["time"] = cdt.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f'{pt["time_requested"]} --> {pt["time"]}')    
        
    return pt

# In[7]:
def result_wind_multi_point(params, windsource):
    results = []
    for pt in params['points']:
        # check time_coverage
        # pt = check_coverage_end(windsource.attrs['time_coverage_end'], pt)
        pt = check_coverage_end(pd.Timestamp(windsource['time'][-2].values).to_pydatetime(), pt)
        
        try:
            # r = get_wind_data(windsource, pt)
            r = get_noaa_data(windsource, pt, 3)
        except Exception as exp:# KeyError:
            logger.info(f'ERROR result_wind_multi_point : {traceback.format_exc()}')   
            r = {'error': "Not Matched"}
        pt.update(r)        
        results.append(pt)
            
    return {'time_coverage_start': windsource.attrs['time_coverage_start'],
            'time_coverage_end': windsource.attrs['time_coverage_end'],
            'points': results}


def result_wave_multi_point(params, wavesource):
    results = []
    for pt in params['points']:
        # check time_coverage
        # pt = check_coverage_end(wavesource.attrs['time_coverage_end'], pt)
        pt = check_coverage_end(pd.Timestamp(wavesource['time'][-2].values).to_pydatetime(), pt)
        
        try:
            # r = get_wave_data(wavesource, pt)
            r = get_noaa_data(wavesource, pt, 1)
        except Exception as exp:# KeyError:
            logger.info(f'ERROR result_wave_multi_point : {exp}')  
            r = {'error': "Not Matched"}
        pt.update(r)        
        results.append(pt)
            
    # return json.dumps({'time_coverage_start': wavesource.attrs['time_coverage_start'],
    #                    'time_coverage_end': wavesource.attrs['time_coverage_end'],
    #                    'points': results})
    return {'time_coverage_start': wavesource.attrs['time_coverage_start'],
            'time_coverage_end': wavesource.attrs['time_coverage_end'],
            'points': results}

def result_current_multi_point(params, currentsource):
    results = []
    for pt in params['points']:     
        # check time_coverage
        # pt = check_coverage_end(currentsource.get_index('time')[-1].strftime("%Y-%m-%dT%H:%M:%SZ"), pt)
        #try:
            #pt = check_coverage_end(pd.Timestamp(currentsource['time'][-2].values).to_pydatetime(), pt)
        #except:
        pt = check_coverage_end(pd.Timestamp(currentsource['time'][-2].values).to_pydatetime(), pt)
        
        try:
            # r = get_wave_data(wavesource, pt)
            r = get_hycom_data(currentsource, pt)
        except Exception as exp:# KeyError:
            logger.info(f'ERROR result_current_multi_point : {exp}') 
            r = {'error': "Not Matched"}
        pt.update(r)        
        results.append(pt)
    
  
    return {'time_coverage_start': currentsource.get_index('time')[0].strftime("%Y-%m-%dT%H:%M:%SZ"),
            'time_coverage_end': currentsource.get_index('time')[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
            'points': results}


app = Flask(name)
@app.route('/')
def index():
    return 'Weather Forecast API'

# multi point data format
'''
JSON input format
{
    "points": [
        {"time": "2022-04-22 21:00:00", "lon": 155.5, "lat": 12},
        ...
        {"time": "2022-04-24 21:00:00", "lon": 157.5, "lat": 18.5}
        ]
}
JSON output format
{
    'time_coverage_start': "2022-04-20 14:00:00",
    'time_coverage_end': "2022-04-25 14:00:00",
    "points": [
        {"time": "2022-04-22 21:00:00", "lon": 155.5, "lat": 12, "ugrd10m": -7.321345, "vgrd10m": 3.23456},
        ...
        {"time": "2022-04-24 21:00:00", "lon": 157.5, "lat": 18.5, "ugrd10m": -7.321345, "vgrd10m": 3.23456}
        ]
}
'''

@app.route('/update/<kind>', methods=['POST'])
def update_weather_data(kind):
    # kind: wind, swell, wwave, twave
    params = request.get_json()
    filename = params['filename']
    print(filename)
    
    if kind == 'wind':
        load_wind(filename)
    elif kind == 'swell':
        load_swell(filename)
    elif kind == 'wwave':
        load_wwave(filename)
    elif kind == 'twave':
        load_twave(filename)
    elif kind == 'current':
        load_current(filename)
    else:
        return f'{kind}: keyword error' 
        
    # 해당 파일명으로 data갱신 처리
    # print(kind, ':', filename)
    # pass
    return f'{kind} data updated'

@app.route('/remove/<kind>', methods=['POST'])
def remove_weather_data(kind):
    # kind: wind, swell, wwave, twave
    try:
        threshold = 2
        latest_files_list = get_file_list()[kind]
        for file_path in sorted(latest_files_list, reverse=True)[threshold:]:
            os.remove(file_path)
            logger.info(f'removed: {file_path}')
    except:
        logger.info(f'{traceback.format_exc()}')
    

# multi point wind data
@app.route('/wind_multi', methods=['POST'])
def post_wind_multi():
    params = request.get_json()
    
    # return result_wind_multi_point(params, gfs)
    wind = result_wind_multi_point(params, gfs)
    return json.dumps({'wind': wind})

# multi point swell data
@app.route('/swell_multi', methods=['POST'])
def post_swell_multi():
    params = request.get_json()

    # return result_wave_multi_point(params, nww3_s)
    swell = result_wave_multi_point(params, nww3_s)
    return json.dumps({'swell': swell})

# multi point windwave data
@app.route('/windwave_multi', methods=['POST'])
def post_windwave_multi():
    params = request.get_json()

    # return result_wave_multi_point(params, nww3_w)
    windwave = result_wave_multi_point(params, nww3_w)
    return json.dumps({'windwave': windwave})

# multi point totalwave data
@app.route('/totalwave_multi', methods=['POST'])
def post_totalwave_multi():
    params = request.get_json()

    # return result_wave_multi_point(params, nww3_T)
    totalwave = result_wave_multi_point(params, nww3_T)
    return json.dumps({'totalwave': totalwave})

# multi point current data
@app.route('/current_multi', methods=['POST'])
def post_current_multi():
    params = request.get_json()

    # return result_current_multi_point(params, hycom)
    current = result_current_multi_point(params, hycom)
    return json.dumps({'current': current})

# multi point all kind of data
@app.route('/allkind_multi', methods=['POST'])
def post_allkind_multi():
    params = request.get_json()

    wind = result_wind_multi_point(params, gfs)
    swell = result_wave_multi_point(params, nww3_s)
    windwave = result_wave_multi_point(params, nww3_w)
    totalwave = result_wave_multi_point(params, nww3_T)
    current = result_current_multi_point(params, hycom)
    return json.dumps({'wind': wind, 'swell': swell, 'windwave': windwave, 'totalwave': totalwave, 'current': current})

def init_api():
    latest_files_dict = get_latest_files() 
    
    load_wind(latest_files_dict['wind'][0])
    load_swell(latest_files_dict['swell'][0])
    load_wwave(latest_files_dict['wwave'][0])
    load_twave(latest_files_dict['twave'][0])
    load_current(latest_files_dict['current'][0])


if name=="main":
    init_api()
    app.run(host='0.0.0.0', port=api_port)