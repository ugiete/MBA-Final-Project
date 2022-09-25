import boto3
import json
import datetime
import requests
import netCDF4 as nc
from os import environ, remove
import logging

logger: logging.Logger = logging.getLogger()

if logger.hasHandlers():
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)

def connect_s3() -> boto3.client:
    """Initiate S3 client

    Returns:
        boto3.client: S3 Client
    """
    return boto3.client('s3')

def download_data() -> bool:
    """Download data from HYCOM (http://ncss.hycom.org/thredds/ncss/grid/GLBv0.08/expt_53.X/data/2012/dataset.html)

    Returns:
        bool: True if download was sucessful, False otherwise
    """
    url: str = 'http://ncss.hycom.org/thredds/ncss/GLBu0.08/expt_19.1/2012'

    params: dict = {
        'var': ['salinity', 'water_temp'],
        'north': environ.get('NORTH_BOUND', 5.272265855051468),
        'west': environ.get('WEST_BOUND', -53.37020817534605),
        'east': environ.get('EAST_BOUND', -25.739999560977726),
        'south':  environ.get('SOUTH_BOUND', -33.74341396166777),
        'disableLLSubset': 'on',
        'disableProjSubset': 'on',
        'horizStride': 1,
        'time_start': f"{environ.get('SEARCH_DATE', '2012-12-31')}T00:00:00Z",
        'time_end': f"{environ.get('SEARCH_DATE', '2012-12-31')}T00:00:00Z",
        'timeStride': 1,
        'vertCoord': None,
        'accept': 'netcdf'
    }
    
    resp: requests.Response = requests.get(url=url, params=params)
    logger.info(f'HYCOM - {resp.status_code}')
    
    if resp.status_code == 200:
        with open('/tmp/data.nc4', 'wb') as fd:
            for chunk in resp.iter_content(100):
                fd.write(chunk)
        
        return True
    else:
        return False

def parse_netcdf() -> str:
    """Parse NetCDF file to JSON

    Returns:
        str: File name generated
    """
    dataset: nc.Dataset = nc.Dataset('/tmp/data.nc4')
    logger.info('READ NC MATRIX')

    salinities: list = dataset['salinity'][:].tolist()[0]
    temperatures: list = dataset['water_temp'][:].tolist()[0]
    latitudes: list = dataset['lat'][:].tolist()
    longitudes: list = dataset['lon'][:].tolist()
    depths: list = dataset['depth'][:].tolist()
    n_depth: int = dataset.dimensions['depth'].size

    data: dict = {}
    
    for lon_idx, lon  in enumerate(longitudes):
        lon_key = str(round(lon, 2))
        data[lon_key] = {}

        for lat_idx, lat in enumerate(latitudes):
            lat_key = str(round(lat, 2))
            data[lon_key][lat_key] = []

            for depth_idx in range(n_depth):
                salinity: float = salinities[depth_idx][lat_idx][lon_idx]
                temperature: float = temperatures[depth_idx][lat_idx][lon_idx]

                data[lon_key][lat_key].append((salinity, temperature))
    
    remove('/tmp/data.nc4')

    logger.info('DELETE NC MATRIX')
    
    now = datetime.datetime.now()
    timestamp_ms = int(now.timestamp() * 1000)
    filename = f'{timestamp_ms}.json'

    with open(f'/tmp/{filename}', 'w') as fd:
        fd.write(json.dumps({
            'meta': {
                'index': {
                    0: 'lon',
                    1: 'lat',
                    2: 'depth'
                },
                'variables': {
                    0: 'salinity',
                    1: 'temperature'
                },
                'limits': {
                    'north': environ.get('NORTH_BOUND', 5.272265855051468),
                    'west': environ.get('WEST_BOUND', -53.37020817534605),
                    'east': environ.get('EAST_BOUND', -25.739999560977726),
                    'south':  environ.get('SOUTH_BOUND', -33.74341396166777),
                },
                'date': f"{environ.get('SEARCH_DATE', '2012-12-31')}T00:00:00Z"
            },
            'depths': depths,
            'data': data
        }))
    
    logger.info(f'BUILD JSON FILE - {filename}')
    
    return filename

def reply(status_code: int, error_message: str = '') -> dict:
    """Generic lambda response

    Args:
        status_code (int): Status code that will be delivered
        error_message (str, optional): Error message to be presented. Defaults to ''.

    Returns:
        dict: Response
    """
    return {
        'statusCode': status_code,
        'body': json.dumps({
            'error': error_message
        })
    }

def main(event, context):
    if download_data():
        json_file_name = parse_netcdf()
        s3_client = connect_s3()

        s3_client.upload_file(f'/tmp/{json_file_name}', environ.get('DATA_BUCKET'), json_file_name)
        logger.info('UPLOAD JSON FILE')

        return reply(status_code=200)
    else:
        return reply(status_code=400, error_message='Fail to download data')