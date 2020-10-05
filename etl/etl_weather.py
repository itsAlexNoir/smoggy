#!/usr/bin/env python
""" etl_weather.py

This scipt loads weather data from sources and load it to a mongo database.
Usage:
    etl_weather.py (-h | --help)
    etl_weather.py --source_path=<path> --port=<port> [--host=<host> --include_stations]

Options:
    -h --help               Show this screen
    --source_path=<path>    Path to the data source
    --host=<host>           Host address of the MongoDB server [default: localhost]
    --port=<Port>           Port of the MongoDB server
    --include_stations      Whether to insert or not station information.
"""

import os
import sys
import logging
from docopt import docopt
from glob import glob
import json
import pandas as pd
import datetime
from tools import database as db


def get_weather_stations(source_path):
    weather_stations_file = os.path.join(source_path, 'estaciones_meteo.json')
    stations = pd.read_json(weather_stations_file)
    #stations['lon'] = stations['longitud'].apply(lambda x: -1 * float(x[:-1]) if x[-1] == 'W' else float(x[:-1]))
    #stations['lon'] = stations['longitud'].apply(lambda x: -1 * float(x[:-1]) if x[-1] == 'W' else float(x[:-1]))
    #with open(weather_stations_file, 'r') as f:
    #    return json.load(f)
    return [sta for sta in stations.to_dict(orient='index').values()]


def get_weather_historic(source_path, wilcard):
    return glob(os.path.join(source_path, wilcard+'*'))


def main(args):

    logging.info('='*80)
    logging.info(' '*20 + 'ETL weather')
    logging.info('=' * 80 + '\n')

    # Connect with the mongo daemon
    logging.info('Connecting to the database client')
    client = db.connect_mongo_daemon(host=args["--localhost"], port=args["--port"])
    # First, create the database. It's dubbed "aire"
    logging.info('Creating weather collection in database')
    weather = db.get_mongo_database(client, 'weather')

    ## CLIMATE INFO
    # Let's import climate data from disk and insert them into the database
    logging.info('Creating climate collection')
    stations_coll = db.get_mongo_collection(weather, 'stations')

    if args["--include_stations"]:
        logging.info('ETL weather stations...')
        stations = get_weather_stations(args["--source_path"])
        result = db.insert_many_documents(weather, 'stations', stations)

    logging.info('Inserting weather data...')
    logging.info('Starting at ' + datetime.datetime.now().strftime('%d/%m/%Y - %H:%m:%s'))
    weather_files = get_weather_historic(args["--source_path"], 'datos_clima*')
    for file in weather_files:
        weather_data = pd.read_json(file)
        weather_data['date'] = pd.to_datetime(weather_data['fecha'], format='%Y-%m-%d')
        weather_dict = [val for val in weather_data.to_dict(orient='index').values()]
        logging.info('Inserting from {} into weather historic collection...'.format(file))
        result = db.insert_many_documents(weather, 'historic', weather_dict)
    logging.info('Creation database finished at ' + datetime.datetime.now().strftime('%d/%m/%Y - %H:%m:%s'))

    logging.info('=' * 80)
    logging.info('Weather info insertion finished')
    logging.info('='*80)


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
