#!/usr/bin/env python
""" request_weather_data.py

"""

__author__ = "Alejandro de la Calle"
__copyright__ = "Copyright 2019"
__credits__ = [""]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "Alejandro de la Calle"
__email__ = "alejandrodelacallenegro@gmail.com"
__status__ = "Development"


import os
import sys
import pandas as pd
import json
from absl import flags, app, logging
import requests
from tools import etl_utils as utils

def define_flags():
    flags.DEFINE_list(name='init_date', default=None, help='Initial date of the request')
    flags.DEFINE_list(name='end_date', default=None, help='Initial date of the request')
    flags.DEFINE_string(name='station', default=None, help='Station info')
    flags.DEFINE_string(name='apikey',default='./api-key', help='Path to the API key')
    flags.DEFINE_string(name='output_path', default='None', help='Path to the output folder')


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
                               'weather_{}_{}.json'.format(station, start.strftime('%Y-%m'))), 'w') as f:
            json.dump(monthly_weather, f)


def main(argv):

    logging.info('=' * 80)
    logging.info(' ' * 20 + 'Request weather info')
    logging.info('=' * 80)

    if FLAGS.init_date is None or FLAGS.end_date is None:
        logging.info('A Initial and end data for requesting data is mandatory. Please provide one.')
        sys.exit(1)

    init_date = utils.get_date(FLAGS.init_date[2], FLAGS.init_date[1], FLAGS.init_date[0])
    end_date = utils.get_date(FLAGS.end_date[2], FLAGS.end_date[1], FLAGS.end_date[0])

    starting_months = pd.date_range(init_date, end_date, freq='MS')
    ending_months = pd.date_range(init_date, end_date, freq='M')

    get_weather_info(starting_months, ending_months, FLAGS.station,
                     FLAGS.output_path, FLAGS.apikey)

    logging.info('=' * 80)
    logging.info('Request finished')
    logging.info('=' * 80)


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
