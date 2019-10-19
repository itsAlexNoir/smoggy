#!/usr/bin/env python
""" etl_traffic.py

This create a mongodb database with air quality, traffic and climate data
available on the open data Madrid City Hall website.
"""

__author__ = "Alejandro de la Calle"
__copyright__ = "Copyright 2019"
__credits__ = [""]
__license__ = ""
__version__ = "0.1"
__maintainer__ = "Alejandro de la Calle"
__email__ = "alejandrodelacallenegro@gmail.com"
__status__ = "Development"

import numpy as np
import pandas as pd
import os
import glob
from pyproj import Proj
import pymongo
from absl import logging, app, flags

from tools import database as db


##################################################################################


def define_flags():
    flags.DEFINE_string(name='source_path', default='.', help='Path to the source of the data.')
    flags.DEFINE_string(name='host', default='localhost', help='Host for Mongo DB database.')
    flags.DEFINE_integer(name='port', default=27019, help='Port connection to Mongo DB database.')

def get_pmed_dataframes_from_paths(path):
    if not os.path.exists(path):
        logging.error('Given path for pmed dataframes does not exists.')
        exit(1)
    else:
        folders = glob.glob(os.path.join(path, 'pmed*'))
        files = [glob.glob(os.path.join(folder, '*csv'))[0] for folder in folders]
        dfs = [pd.read_csv(file, delimiter=';', encoding='latin1') for file in files]
    return dict(zip(files, dfs))


def insert_traffic_density_dataframes_to_db(path, database, coll):
    if not os.path.exists(path):
        logging.error('Given path for density dataframes does not exists.')
        exit(1)
    else:
        files = glob.glob(os.path.join(path, '*csv'))
        chunksize = 10 ** 4
        logging.info('Inserting dataframes...')
        for file in files:
            logging.info('Loading dataframe from ' + file)
            logging.info('Processing dataframes with chunk size: {}'.format(chunksize))
            df_chunk = pd.read_csv(file, delimiter=';', encoding='latin1', chunksize=chunksize)
            for chunk_id, chunk in enumerate(df_chunk):
                if chunk_id % 100 ==0:
                    logging.info('Processing chunk number {}'.format(chunk_id))
                chunk['date'] = pd.to_datetime(chunk['fecha'])
                chunk.drop(columns='fecha', inplace=True)
                density = [entry for entry in chunk.to_dict(orient='index').values()]
                db.insert_many_documents(database, coll, density)


def get_location_for_pmeds(dfs):
    # Define the projector, to transform from UTM to lat/lon coordinates
    myProj = Proj("+proj=utm +zone=30N, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    # Convert all dataframes into dicts
    dicts = [df.to_dict(orient='index') for df in dfs.values()]
    # Iterate. Look for the coordinates columns. Then add a key
    # with the coordinates (in UTM) in each of the entries.
    for file, df, dd in zip(dfs.keys(), dfs.values(), dicts):
        logging.info('Extracting from ' + file)
        xcol = None
        ycol = None
        for col in df.columns:
            if ("x" in col) or ("X" in col):
                xcol = col
            elif ("y" in col) or ("Y" in col):
                ycol = col
        if (xcol is not None) and (ycol is not None):
            if df[xcol].dtype != 'float64':
                xutm = df[xcol].apply(lambda x: x.replace(',', '.')).astype(np.float64).values
            else:
                xutm = df[xcol].values
            if df[ycol].dtype != 'float64':
                yutm = df[ycol].apply(lambda x: x.replace(',', '.')).astype(np.float64).values
            else:
                yutm = df[ycol].values

            if np.isnan(xutm).any():
                logging.info('NaN value found at ' + file)
                continue
            if np.isnan(yutm).any():
                logging.info('NaN value found at ' + file)
                continue
            lon, lat = myProj(xutm, yutm, inverse=True)
            for idx in df.index:
                dd[idx]['location'] = {"coordinates": [lon[idx], lat[idx]], "type": "Point"}
    # Return the dictionaries
    return dicts


def main(argv):
    logging.info('Initializing ETL for traffic data...')
    logging.info('------------------------------------\n')

    logging.info('Get measure points dataframes from path')
    dfs = get_pmed_dataframes_from_paths(os.path.join(FLAGS.source_path,
                                                      'ubicacion_puntos_medida', 'ubicacion'))

    logging.info('Get pmed dictionaries with location info')
    pmed_dicts = get_location_for_pmeds(dfs)

    # Connect with the mongo daemon
    client = db.connect_mongo_daemon(host=FLAGS.host, port=FLAGS.port)
    logging.info('Creating traffic database')
    traffic = db.get_mongo_database(client, 'traffic')

    logging.info('Creating pmed (measure points) collection for traffic database')
    pmed_coll = db.get_mongo_collection(traffic, 'pmed')

    logging.info('Inserting entries from dataframes')
    for dd in pmed_dicts:
        result = db.insert_many_documents(traffic, 'pmed', [d for d in dd.values()])
    logging.info('Insertion ended')
    logging.info('Creating geospatial index...')
    pmed_coll.create_index([("location", pymongo.GEOSPHERE)])
    logging.info('------------------------------------\n')

    logging.info('Processing density collection for traffic database')
    density_coll = db.get_mongo_collection(traffic, 'density')
    insert_traffic_density_dataframes_to_db(os.path.join(FLAGS.source_path,
                                            'intensidad_trafico', 'csv'),
                                            traffic, 'density')

    # Create a compound index for effcient quering
    density_coll.create_index([("date", -1), ("id", 1)], name='traffic_index')
    logging.info('ETL process finished!')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
