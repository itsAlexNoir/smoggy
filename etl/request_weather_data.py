#!/usr/bin/env python
""" request_weather_data.py
Usage:
    request_weather_data.py (-h | --help)
    request_weather_data.py --init_date=<dd-mm-YYYY> --end_date=<dd-mm-YYYY> --station=<name> --apikey=<string> --out_path=<path> 

Options:
    -h --help                   Show this screen.
    --init_date=<dd-mm-YYYY>    Initial date of the request
    --end_date=<dd-mm-YYYY>     Final date of the request
    --station=<name>            Name of the requested weather station
    --apikey=<string>           Path to the API key to access the server [default: ./api-key]
    --out_path=<path>           Path to the output folder, in which data should be downloaded.
"""
import os
import sys
import pandas as pd
import json
from docopt import docopt
import logging
import requests

from tools import etl_utils as utils


def request_climate_info(init_date, end_date, station=None, apikey=None):
    if apikey is None:
        logging.info('An API-key is needed for the request')
        sys.exit(1)
    if station is None:
        logging.info('Please provide a weather station')
        sys.exit(1)

    preurl = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini"
    url = os.path.join(preurl, init_date.strftime('%Y-%m-%dT%H:%M:%SUTC'), 'fechafin',
                       end_date.strftime('%Y-%m-%dT%H:%M:%SUTC'), 'estacion', utils.estaciones_meteo[station])
    # Get AEMET API key
    with open(apikey, 'r') as f:
        key = f.readlines()[0]

    querystring = {"api_key": key}
    logging.info('Requesting climate info between {} and {} for station {}'.format(init_date, end_date, station))
    response = requests.get(url, params=querystring)
    if response.json()['estado'] != 200:
        logging.error('Climate request has an error: {}'.format(response.json()['descripcion']))
    weather_info = requests.get(response.json()['datos'])
    return weather_info.json()


def get_weather_info(start_date, end_date, station, output_path, apikey):
    for start, end in zip(start_date, end_date):
        print('Requesting from {} to {}'.format(start, end))
        monthly_weather = request_climate_info(start, end, station, apikey)
        with open(os.path.join(output_path,
                               'datos_clima_{}_{}.json'.format(station, start.strftime('%Y-%m'))), 'w') as f:
            json.dump(monthly_weather, f)


def main(args):

    logging.info('=' * 80)
    logging.info(' ' * 20 + 'Request weather info')
    logging.info('=' * 80)

    if args["--init_date"] is None or args["--end_date"] is None:
        logging.info('A Initial and end data for requesting data is mandatory. Please provide one.')
        sys.exit(1)        

    init_date = pd.to_datetime(args["--init_date"], format='%d-%m-%Y')
    end_date = pd.to_datetime(args["--end_date"], format='%d-%m-%Y')

    starting_months = pd.date_range(init_date, end_date, freq='MS')
    ending_months = pd.date_range(init_date, end_date, freq='M')

    get_weather_info(starting_months, ending_months, args["--station"],
                     args["--out_path"], args["--apikey"])

    logging.info('=' * 80)
    logging.info('Request finished')
    logging.info('=' * 80)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
