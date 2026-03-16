#-*- coding:utf-8 -*-
import cdsapi as cds
import sys
import pandas as pd
import argparse
import json
import os
from dataclasses import dataclass
import logging
import shutil
import calendar


# ## logging 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ecmwf_download.log'),
        # logging.StreamHandler(sys.stdout)
    ]
)


class Collection:

    @staticmethod
    def wind(year, month, day, hour, c):
        # Check if the date is valid
        _, last_day = calendar.monthrange(year, month)
        if day > last_day:
            logging.info(f'Skipping invalid date: {year:04d}-{month:02d}-{day:02d}')
            return
            
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "10m_u_component_of_wind",
                "10m_v_component_of_wind"
            ],
            "year": year,
            "month": month,
            "day": day,
            "time": hour, #+":00",
            "data_format": "netcdf",
            "download_format": "unarchived",
            #"area": area
        }
        # name= 'ecmwf_wind_{0:04d}{1:02d}{2:2d} {3}h.nc'.format(year,month,day,hour)  # data type, point num.,
        name=f'ecmwf_wind_{year:04d}{month:02d}{day:02d}_{hour:02d}h.nc'
        
        # Check if file already exists in target directory
        target_dir = os.path.join(f"{year:04d}", f"{month:02d}")
        destination_path = os.path.join(target_dir, name)
        
        if os.path.exists(destination_path):
            logging.info(f'File {name} already exists in {target_dir}, skipping download')
            return
            
        # print(request)
        c.retrieve(dataset, request, name)
        logging.info(f'download of {name} done')

        # Move the downloaded file to the target directory
        try:
            os.makedirs(target_dir, exist_ok=True)
            shutil.move(name, destination_path) # Use shutil.move for safer moving
            logging.info(f'Moved {name} to {destination_path}')
        except Exception as e:
            logging.error(f"Failed to move {name}: {e}")

    @staticmethod
    def wave(year, month, day, hour, c):
        # Check if the date is valid
        _, last_day = calendar.monthrange(year, month)
        if day > last_day:
            logging.info(f'Skipping invalid date: {year:04d}-{month:02d}-{day:02d}')
            return
            
        dataset = "reanalysis-era5-single-levels"
        request = {
            "product_type": ["reanalysis"],
            "variable": [
                "mean_wave_direction",
                "mean_wave_period",
                "significant_height_of_combined_wind_waves_and_swell"
            ],
            "year": year,
            "month": month,
            "day": day,
            "time": hour, #+":00",
            "data_format": "netcdf",
            "download_format": "unarchived",
            #"area": area
        }
        # name= 'ecmwf_wind_{0:04d}{1:02d}{2:2d} {3}h.nc'.format(year,month,day,hour)  # data type, point num.,
        name=f'ecmwf_wave_{year:04d}{month:02d}{day:02d}_{hour:02d}h.nc'
        
        # Check if file already exists in target directory
        target_dir = os.path.join(f"{year:04d}", f"{month:02d}")
        destination_path = os.path.join(target_dir, name)
        
        if os.path.exists(destination_path):
            logging.info(f'File {name} already exists in {target_dir}, skipping download')
            return
            
        # print(request)
        c.retrieve(dataset, request, name)
        logging.info(f'download of {name} done')
        
        # Move the downloaded file to the target directory
        try:
            os.makedirs(target_dir, exist_ok=True)
            shutil.move(name, destination_path) # Use shutil.move for safer moving
            logging.info(f'Moved {name} to {destination_path}')
        except Exception as e:
            logging.error(f"Failed to move {name}: {e}")


@dataclass
class Params:
    class Year:
        start: int
        end: int
        step: int
    
    class Month:
        start: int
        end: int
        step: int

    class Day:
        start: int
        end: int
        step: int

    class Hour:
        start: int
        end: int
        step: int
    
    type : str


def load_configuration():
    param_path = 'down.json'
    params = Params

    try:
        if not os.path.exists(param_path):
            raise FileNotFoundError('{} ddse not exist'.format(param_path))

    except FileNotFoundError as err:
        print(err)

    with open(param_path) as f:
        loaded = json.load(f)
        years = loaded['year']
        months = loaded['month']
        days = loaded['day']
        hours = loaded['hour']
        params.Year.start = years['start']
        params.Year.end = years['end']
        params.Year.step = years['step']
        params.Month.start = months['start']
        params.Month.end = months['end']
        params.Month.step = months['step']
        params.Day.start = days['start']
        params.Day.end = days['end']
        params.Day.step = days['step']
        params.Hour.start = hours['start']
        params.Hour.end = hours['end']
        params.Hour.step = hours['step']
        params.type = loaded["type"]
    return params


def load_route():
    route_path = 'route.json'

    try:
        if not os.path.exists(route_path):
            raise FileNotFoundError('{} dose not exist'.format(route_path))

    except FileNotFoundError as err:
        print(err)

    
    with open(route_path) as f:
        loaded = json.load(f)
        route = loaded["route"]

    return route


def args_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", type=str, default="wind", choices=["wind", "wave"])
    parser.add_argument("--step", type=int, default=3, help="step size for hour")
    return parser.parse_args()

if __name__ == '__main__':

    # Year = sys.argv[1]  ## ["2021",...,"2025"]    
    # Month = sys.argv[2] ## ["01","02","03",...,"12"]
    # Day = sys.argv[3]   ## ["01","02","03",...,"31"]
    # Hour = sys.argv[4]  ## ["00:00", "01:00", "02:00", ... , "23:00"]
    # Type = sys.argv[5]  ## "wind"
    # Area = sys.argv[6]  ## [North, West, South, East]

    args = args_parse()

    params = load_configuration()
    # route = load_route()

    client = cds.Client(
        url="https://cds.climate.copernicus.eu/api",
        key="<UID>:a6e4a459-f5d9-4073-85c0-8c728bb2a592",
        #verify=True,  # 사내 CA 필요 시 verify="/path/to/corp-ca.pem"
        #timeout=600,  # 상황에 따라 연장 가능
    )


    # Year = [f"{x:04d}" for x in range(params.year.start, params.year.end + 1, params.year.step)]
    # Month = [f"{x:02d}" for x in range(params.month.start, params.month.end + 1, params.month.step)]
    # Day = [f"{x:02d}" for x in range(params.day.start, params.day.end + 1, params.day.step)]
    # Hour = [f"{x:02d}:00" for x in range(params.hour.start, params.hour.end + 1, params.hour.step)]

    logging.info("process started...")

    for yy in range(params.Year.start, params.Year.end + 1, params.Year.step):
        Year = [f"{yy:04d}"]

        for mm in range(params.Month.start, params.Month.end + 1, params.Month.step):
            Month= [f"{mm:02d}"]

            for dd in range(params.Day.start, params.Day.end + 1, params.Day.step):
                Day = [f"{dd:02d}"]

                for hh in range(params.Hour.start, params.Hour.end + 1, params.Hour.step):
                    Hour = [f"{hh:02d}:00"]

                    print(f"\rdownloading: {yy:04d}-{mm:02d}-{dd:02d} {hh:02d}h", end='', flush=True)
                    try:
                        if args.type == "wind":
                            Collection.wind(yy, mm, dd, hh, client)
                        elif args.type == "wave":
                            Collection.wave(yy, mm, dd, hh, client)

                    except KeyboardInterrupt:
                        logging.info("process terminated: user interrupted")
                        break

                    except Exception as ex:
                        logging.error(f"unexpected error: {str(ex)}", exc_info=True)

    logging.info("process done!!")