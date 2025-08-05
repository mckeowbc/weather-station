#!/usr/bin/env python3

import requests
import os
import sys
import re


creds_file       = sys.argv[1]
metrics_endpoint = sys.argv[2]

with open(creds_file) as fin:
    creds_string = fin.readline()

    m = re.match(r'^ID:(?P<ID>[^,]+),KEY:(?P<PASSWORD>.*)', creds_string)

    if not m:
        raise ValueError(f"Invalid credentials: {creds_string}")
    creds = m.groupdict()

r = requests.get(metrics_endpoint)

if r.status_code != 200:
    raise RuntimeError(f"Got error: {r.status_code}")


measurement = r.content.decode('utf-8')

MAPPING = {
    'temperature'    : 'tempf',
    'humidity'       : 'humidity',
    'rain_in'        : 'dailyrainin',
    'wind_direction' : 'win_dir',
}


URL = 'https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php'

lines = measurement.split('\n')

mdict = { 'action' : 'updateraw', 'dateutc' : 'now'}

for line in lines:
    try:
        (metric, measurement) = re.split(r'\s+', line.strip())

        if metric in MAPPING:
            mdict[MAPPING[metric]] = measurement
        if metric == 'wind_speed':
            measurement = float(measurement) * 0.621371
            mdict['windspeedmph'] = str(measurement)
    except:
        pass

mdict.update(creds)

query_string = '&'.join(f'{key}={val}' for key, val in mdict.items())


r = requests.get(f'{URL}?{query_string}')

print(r.status_code)
print(r.content.decode('utf-8'))