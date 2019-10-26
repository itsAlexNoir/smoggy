import os
import sys
import glob
import pandas as pd
from absl import app, logging, flags
from tools import database as db
from tools.etl_utils import insert_pollution_docs_to_db


def define_flags():
    flags.DEFINE_string(name='source_path', default='.', help='Path to the source of the data')


def get_air_stations(source_path):
    stations = pd.read_csv(os.path.join(source_path, 'estaciones', 'pollution_stations.csv'),
                           encoding='latin1', delimiter=';')
    stations.drop(columns=stations.columns[0], inplace=True)
    return [val for val in stations.to_dict(orient='index').values()]


def main(argv):

    logging.info('='*80)
    logging.info(' '*20 + 'ETL air quality')
    logging.info('=' * 80)

    if FLAGS.source_path is None:
        logging.error('Source path for input data must be provided.')
        sys.exit(1)
    logging.info('Data sourced from :' + FLAGS.source_path)

    # Put into the database
    logging.info('Connecting with the database')
    client = db.connect_mongo_daemon(host='localhost', port=27019)
    pollutiondb = db.get_mongo_database(client, 'pollution')

    # Load station info
    logging.info('Inserting pollution stations data...')
    stations = get_air_stations(FLAGS.source_path)
    result = db.insert_many_documents(pollutiondb, 'stations', stations)

    # First, all the data are in txt. It must be converted to csv
    # for easier manipulation
    insert_pollution_docs_to_db(os.path.join(FLAGS.source_path, 'contaminantes',  'historico'),
                                pollutiondb, 'pollutants')

    # Create index
    pollutants_coll = db.get_mongo_collection(pollutiondb, 'pollutants')
    pollutants_coll.create_index([("dates", -1), ("magnitud", 1)], name='pollutant_index')
    pollutants_coll.create_index([("station", 1), ("magnitud", 1)], name='pollutant_plus_station_index')
    pollutants_coll.create_index([("station", 1), ("dates", -1), ("magnitud", 1)], name='pollutant_all_index')

    # Closing script
    logging.info('ETL air_quality process finished.')
    logging.info('=' * 80)


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
