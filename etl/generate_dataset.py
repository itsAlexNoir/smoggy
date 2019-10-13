#!/usr/bin/env python
""" generate_dataset.py

This module contain routines for creating and interacting with a mongodb database.
"""

import os
import datetime
import pandas as pd
from absl import flags, app
import logging

from tools import database as db

# lon/lat coordinates for Puerta del Sol (Madrid city central point)
PUERTA_SOL = [-3.703339, 40.416729]


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
    for station in stations:
        logging.info('Pulling nearest stations for ' + station['ESTACION'])
        closest[station['CODIGO_CORTO']] = {}
        for key, dist in distances.items():
            pmed_list = [query for query in pmed_coll.find({"location":
                                        {"$near":
                                            {"$geometry": {"type": "Point", "coordinates": [station['LONGITUD'],
                                                                                            station['LATITUD']]},
                                            "$minDistance": dist[0],
                                            "$maxDistance": dist[1]
                                            }
                                        }
                                    })]
            if len(list(pmed_list)) != 0:
                closest[station['CODIGO_CORTO']][key] = pd.DataFrame(list(pmed_list))['id'].dropna().unique()
    return closest


def get_density_traffic_data(database, start, end, close):
    density_coll = database['traffic']['density']

    one_hour = pd.Timedelta(hours=1)
    dataset_duration = (end - start).days * 24
    density = []
    for ihour in range(dataset_duration):
        query_date = start + one_hour * ihour
        logging.info('Getting traffic data for '+query_date.strftime('%d-%m-%Y %H:%M:%s'))
        for station in close.keys():
            logging.info('For pollution station code {}'.format(station))
            for dist in close[station].keys():
                density_hour = pd.DataFrame([den for den in density_coll.find({"date": query_date,
                                                                  "id": {"$in": list(map(int,
                                                                                         close[station][dist]))}})])
                density.append({'date': query_date,
                 'intensidad_media': density_hour['intensidad'].mean(),
                 'intensidad_min': density_hour['intensidad'].min(),
                 'intensidad_max': density_hour['intensidad'].max(),
                 'carga_media': density_hour['carga'].mean(),
                 'carga_min': density_hour['carga'].min(),
                 'carga_max': density_hour['carga'].max(),
                 'ocupacion_media': density_hour['ocupacion'].mean(),
                 'ocupacion_min': density_hour['ocupacion'].min(),
                 'ocupacion_max': density_hour['ocupacion'].max(),
                 'vmed_media': density_hour['vmed'].mean(),
                 'vmed_min': density_hour['vmed'].min(),
                 'vmed_max': density_hour['vmed'].max()})

    print('hhhggggg')

def main(argv):
    logging.info('_'*80)
    logging.info('Generate dataset!')
    logging.info('_' * 80 + '\n')

    start_date = datetime.datetime(2018, 1, 1)
    end_date = datetime.datetime(2019, 1, 1)

    # Connect to the database
    client = db.connect_mongo_daemon(host='localhost', port=27019)

    # Retrieving calendar info
    calendar_df = get_calendar_features(client, start_date, end_date)

    # Weather info
    #weather_df = get_weather_features(client, start_date, end_date)

    # Get all traffic pmed in a certain radius from pollution station
    distances = {'near': [0, 500], 'medium': [500, 1000], 'far': [1000, 1500]}
    closest = get_nearby_traffic_pmed(client, distances)


    # Extract traffic density data
    logging.info('Retrieve info for a density info')
    get_density_traffic_data(client, start_date, end_date, closest)

    # df = pd.DataFrame({'Date': calendar_date, 'Festivo': one_hot_festivo})
    logging.info('_'*80)
    logging.info('Finished dataset generation!')
    logging.info('_' * 80 + '\n')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    #define_apps()
    app.run(main)