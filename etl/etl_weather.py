#!/usr/bin/env python
""" etl_weather.py

This scipt loads weather data from sources and load it to a mongo database.
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
import glob
import json
import datetime
from absl import flags, app, logging
from tools import database as db

def define_flags():
    flags.DEFINE_string(name='source_path', default=None, help='Path to the source of the data')


def get_weather_stations(source_path):
    clima_stations_file = os.path.join(source_path, 'estaciones_meteo.json')
    with open(clima_stations_file, 'r') as f:
        return json.load(f)

def main(argv):

    logging.info('='*80)
    logging.info(' '*20 + 'ETL weather')
    logging.info('=' * 80 + '\n')

    if FLAGS.source_path is None:
        logging.error('A path to the data must be provided')
        sys.exit(1)

    # Connect with the mongo daemon
    logging.info('Connecting to the database client')
    client = db.connect_mongo_daemon()
    # First, create the database. It's dubbed "aire"
    logging.info('Creating weather collection in database')
    weather = db.get_mongo_database(client, 'weather')

    ## CLIMATE INFO
    # Let's import climate data from disk and insert them into the database
    logging.info('Creating climate collection')
    historic = db.get_mongo_collection(weather, 'historic')

    logging.info('ETL weather stations...')
    stations = get_weather_stations(FLAGS.source_path)
    result = db.insert_many_documents(weather, 'historic', stations)

    # clima_files = glob.glob(os.path.join(FLAGS.source_path, 'datos_clima*'))

    logging.info('Inserting weather data...')
    logging.info('Starting at ' + datetime.datetime.now().strftime('%d/%m/%Y - %H:%m:%s'))

    #
    # print('Writing ...')
    # for file in clima_files:
    #     with open(file, 'r') as f:
    #         clima_data = json.load(f)
    #     print('Inserting from {} into clima collection...'.format(file))
    #     result = db.insert_many_documents(aire, 'clima', clima_data)
    #

    logging.info('=' * 80)
    logging.info('Creation database finished at ' + datetime.datetime.now()).strftime('%d/%m/%Y - %H:%m:%s')
    logging.info('='*80)

if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
