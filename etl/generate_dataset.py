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

def main(argv):
    logging.info('_'*80)
    logging.info('Generate dataset!')
    logging.info('_' * 80 + '\n')

    start_date = datetime.datetime(2018, 1, 1)
    end_date = datetime.datetime(2019, 1, 1)

    # Connect to the database
    client = db.connect_mongo_daemon(host='localhost', port=27019)

    # Retrieving calendar info
    calendar_coll = client['calendar']['calendar']
    calendar = [cal for cal in calendar_coll.find({"date": {"$gte": start_date, "$lte": end_date}})]
    one_hot_festivo = pd.Series([0 if cal['laborable / festivo / domingo festivo'] == 'laborable' else 1 for cal in calendar])
    calendar_date = pd.Series(['{:02d}-{:02d}-{:4d}'.format(cal["Día"], cal["Mes"], cal["Año"]) for cal in calendar])
    calendar_date = pd.to_datetime(calendar_date, format='%d-%m-%Y')

    # Weather info
    #weather_coll = client['weather']['historic']
    #weather = [cal for cal in weather_coll.find({"date": {"$gte": start_date, "$lte": end_date}})]

    #
    stations_coll = client['pollution']['stations']
    stations = [st for st in stations_coll.find()]
    #df = pd.DataFrame({'Date': calendar_date, 'Festivo': one_hot_festivo})

    pmed_coll = client['traffic']['pmed']
    closest = {}
    distances = {'near': [0, 500], 'medium': [500, 1000], 'far': [1000, 1500]}
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

    logging.info('_'*80)
    logging.info('Finished dataset generation!')
    logging.info('_' * 80 + '\n')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    #define_apps()
    app.run(main)