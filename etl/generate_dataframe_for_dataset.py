#!/usr/bin/env python
""" generate_dataset.py

This module contain routines for creating and interacting with a mongodb database.
"""

import os
import datetime
import pandas as pd
from absl import flags, app
import logging
import time
import pickle
from tqdm import tqdm
from functools import partial
import multiprocessing as mp
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from tools import database as db

# lon/lat coordinates for Puerta del Sol (Madrid city central point)
PUERTA_SOL = [-3.703339, 40.416729]


def define_flags():
    flags.DEFINE_list(name='start_date', default=None, help='Initial date of the request')
    flags.DEFINE_list(name='end_date', default=None, help='Initial date of the request')
    flags.DEFINE_string(name='output_folder', default=None, help='Path to the output folder')


def get_calendar_features(database, start, end):
    calendar_coll = database['calendar']['calendar']
    calendar = [cal for cal in calendar_coll.find({"date": {"$gte": start, "$lte": end}})]
    one_hot_festivo = pd.Series(
        [0 if cal['laborable / festivo / domingo festivo'] == 'laborable' else 1 for cal in calendar])
    calendar_date = pd.Series(['{:02d}-{:02d}-{:4d}'.format(cal["Día"], cal["Mes"], cal["Año"]) for cal in calendar])
    calendar_date = pd.to_datetime(calendar_date, format='%d-%m-%Y')
    return pd.DataFrame({'Date': calendar_date, 'Festivo': one_hot_festivo})


def get_weather_features(database, start, end):
    weather_coll = database['weather']['historic']
    weather = [cal for cal in weather_coll.find({"date": {"$gte": start, "$lte": end}})]


def get_nearby_traffic_pmed(database, distances):
    # Load pollution stations
    stations_coll = database['pollution']['stations']
    stations = [st for st in stations_coll.find()]

    # Load traffic measure points
    pmed_coll = database['traffic']['pmed']
    closest = {}
    for station in tqdm(stations):
        logging.info('Pulling nearest stations for ' + station['ESTACION'])
        closest[station['CODIGO_CORTO']] = {}
        for key, dist in distances.items():
            pmed_list = [query for query in pmed_coll.find({"location":
                                                                {"$near":
                                                                     {"$geometry": {"type": "Point",
                                                                                    "coordinates": [station['LONGITUD'],
                                                                                                    station[
                                                                                                        'LATITUD']]},
                                                                      "$minDistance": dist[0],
                                                                      "$maxDistance": dist[1]
                                                                      }
                                                                 }
                                                            })]
            if len(list(pmed_list)) != 0:
                closest[station['CODIGO_CORTO']][key] = pd.DataFrame(list(pmed_list))['id'].dropna().unique()
    return closest


def retrieve_traffic_data_by_hour(query_date, close, coll):
    density = []
    #logging.info('Getting traffic data for ' + query_date.strftime('%d-%m-%Y %H:%M:%s'))
    for station in close.keys():
        #logging.info('For pollution station code {}'.format(station))
        dense_row = {}
        for dist in close[station].keys():

            try:
                density_hour = pd.DataFrame([den for den in coll.find({"date": query_date,
                                                                            "id": {"$in": list(map(int,
                                                                                                   close[station][
                                                                                                      dist]))}})])
                dense_row.update({'date': query_date, 'reference_station': station,
                            'intensidad_media_' + dist: density_hour['intensidad'].mean(),
                            'intensidad_min_' + dist: density_hour['intensidad'].min(),
                            'intensidad_max_' + dist: density_hour['intensidad'].max(),
                            'carga_media_' + dist: density_hour['carga'].mean(),
                            'carga_min_' + dist: density_hour['carga'].min(),
                            'carga_max_' + dist: density_hour['carga'].max(),
                            'ocupacion_media_' + dist: density_hour['ocupacion'].mean(),
                            'ocupacion_min_' + dist: density_hour['ocupacion'].min(),
                            'ocupacion_max_' + dist: density_hour['ocupacion'].max(),
                            'vmed_media_' + dist: density_hour['vmed'].mean(),
                            'vmed_min_' + dist: density_hour['vmed'].min(),
                            'vmed_max_' + dist: density_hour['vmed'].max()})
            except KeyError:
                logging.error('Exception detected for ' + query_date.strftime("%Y-%m-%d %H:%M:%S"))
                pass
        density.append(dense_row)
    return density


def get_density_traffic_data(database, start, end, close):
    density_coll = database['traffic']['density']
    time_axis = pd.date_range(start=start, end=end, freq='H')
    mapfunc = partial(retrieve_traffic_data_by_hour, close=close, coll=density_coll)
    logging.info('Starting retrieving traffic data at ' + datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    with ThreadPool() as pool:
        density = pool.map_async(mapfunc, time_axis)
        while not density.ready():
            time.sleep(0.01)
    logging.info('Ending retrieving traffic data at ' + datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    return [item for sublist in density.get() for item in sublist]


def get_air_quality_data(database, start, end):
    air_coll = database['pollution']['pollutants']
    time_axis = pd.date_range(start=start, end=end, freq='H')
    interesting_cols = ['station', 'magnitud', 'date', 'value']
    air_data = []
    for query_date in time_axis:
        # air = pd.DataFrame(list(air_coll.find({"dates": query_date, "station": 4, "magnitud": 8}))).rename(
        #     columns={"dates": "date"})
        air = pd.DataFrame(list(air_coll.find({"dates": query_date}))).rename(
            columns={"dates": "date"})
        air_data.append(air[interesting_cols])
    return pd.concat([air for air in air_data], ignore_index=True)

def main(argv):
    logging.info('_' * 80)
    logging.info('Generate dataset!')
    logging.info('_' * 80 + '\n')

    start_date = pd.to_datetime('{}-{}-{}'.format(FLAGS.start_date[1], FLAGS.start_date[1], FLAGS.start_date[2]),
                                format='%d-%m-%Y')
    end_date = pd.to_datetime('{}-{}-{}'.format(FLAGS.end_date[0], FLAGS.end_date[1], FLAGS.end_date[2]),
                              format='%d-%m-%Y')

    # Connect to the database
    client = db.connect_mongo_daemon(host='localhost', port=27019)

    # Retrieving calendar info
    calendar_df = get_calendar_features(client, start_date, end_date)

    # Weather info
    #weather_df = get_weather_features(client, start_date, end_date)

    # Get air quality data
    logging.info('Retrieve info for air quality')
    air_data = get_air_quality_data(client, start_date, end_date)

    air_data.to_pickle(os.path.join(FLAGS.output_folder, 'air_df.pkl'))
    #with open(os.path.join(FLAGS.output_folder, 'air_df.pkl'), 'w') as f:
    #    pickle.dump(air_data, f)

    # Get all traffic pmed in a certain radius from pollution station
    distances = {'0_500m': [0, 500], '500m_1km': [500, 1000]}
    #closest = get_nearby_traffic_pmed(client, distances)

    # Extract traffic density data
    logging.info('Retrieve info for a density info')
    #traffic_data = get_density_traffic_data(client, start_date, end_date, closest)


    # TO DO
    # Join all the pieces all together
    #calendar_df.rename(columns={'Date': 'date'}).set_index('date').join(air_data.set_index('date'))
    #calendar_df.join(weather_df, on='date')
    logging.info('_' * 80)
    logging.info('Finished dataset generation!')
    logging.info('_' * 80 + '\n')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
