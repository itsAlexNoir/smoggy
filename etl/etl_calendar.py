import os
import sys
import pandas as pd
from tools import database as db
from absl import app, flags, logging

def define_flags():
    flags.DEFINE_string('source_path', default=None, help='Path to the data source')

def get_calendar_from_source(source_path):
    df = pd.read_csv(os.path.join(source_path, 'calendario.csv'),
                     encoding='latin1', delimiter=';')
    df['Fecha'] = df['Dia'].apply(lambda x: pd.to_datetime(x, format='%d/%M/%Y'))
    df['Día'] = df['Fecha'].apply(lambda x: x.day)
    df['Mes'] = df['Fecha'].apply(lambda x: x.month)
    df['Año'] = df['Fecha'].apply(lambda x: x.year)

    return [val for val in df.drop(columns=['Fecha']).to_dict(orient='index').values()]


def main(argv):
    logging.info('='*80)
    logging.info(' '*20 + 'ETL calendar')
    logging.info('=' * 80)

    if FLAGS.source_path is None:
        logging.error('Source path for input data must be provided.')
        sys.exit(1)
    logging.info('Data sourced from :' + FLAGS.source_path)

    logging.info('Extracting data...')
    calendar = get_calendar_from_source(FLAGS.source_path)

    # Put into the database
    client = db.connect_mongo_daemon(host='localhost', port=27019)
    calendardb = db.get_mongo_database(client, 'calendar')
    logging.info('Inserting calendar data...')
    result = db.insert_many_documents(calendardb, 'calendar', calendar)


    logging.info('ETL calendar process finished.')
    logging.info('=' * 80)


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
