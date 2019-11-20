import os
import numpy as np
import pandas as pd
import pickle
from glob import glob
from tqdm import tqdm
from absl import app, flags
import logging
#from ptools import plotting


def define_flags():
    flags.DEFINE_string(name='source_folder', default=None, help='Path to the source of the data')
    flags.DEFINE_string(name='output_folder', default=None, help='Output folder for dataset')


def univariate_data(dataset, start_index, end_index, history_size, target_size):
    data = []
    labels = []

    start_index = start_index + history_size
    if end_index is None:
        end_index = len(dataset) - target_size

    for i in range(start_index, end_index):
        indices = range(i-history_size, i)
        # Reshape data from (history_size,) to (history_size, 1)
        data.append(np.reshape(dataset[indices], (history_size, 1)))
        labels.append(dataset[i+target_size])
    return np.array(data), np.array(labels)


def multivariate_data(dataset, target, start_index, end_index, history_size,
                      target_size, step, single_step=False):
  data = []
  labels = []

  start_index = start_index + history_size
  if end_index is None:
    end_index = len(dataset) - target_size

  for i in range(start_index, end_index):
    indices = range(i-history_size, i, step)
    data.append(dataset[indices])

    if single_step:
      labels.append(target[i+target_size])
    else:
      labels.append(target[i:i+target_size])

  return np.array(data), np.array(labels)


def main(argv):
    logging.info('='*80)
    logging.info(' ' * 20 + 'Create dataset')
    logging.info('=' * 80 + '\n')

    logging.info('Reading dataframe from pickle...')
    df_files = glob(os.path.join(FLAGS.source_folder, '*pkl'))
    df = pd.concat([pd.read_pickle(file) for file in tqdm(df_files)], ignore_index=True)
    pollutant_list = ['SO2', 'CO', 'NO', 'NO2', 'NOx']

    # logging.info('Pivot all the pollutant so they can be features')
    # df_per_pollutant = {pol: df[df['magnitud']==pol] for pol in pollutant_list}
    # target_df = df_per_pollutant[8].drop(columns='magnitud')
    # #for pol in pollutant_list:
    # #    target_df[pol] = df_per_pollutant[pol]['value']

    #for col in df.columns:
    #    if target_df[col].dtype == np.object:
    #        target_df[col] = target_df[col].apply(lambda x: np.float32(x.replace(',', '.')) if not isinstance(x, float) else x)

    logging.info('Get some parameters of the dataset...')
    #df.apply(lambda x: x.astype(np.float32))
    n_obs = len(df.index)
    test_split = int(n_obs * 0.15)
    train_split = n_obs - test_split

    logging.info('=====================================')
    logging.info(' '*10 + 'Parameters')
    logging.info('=====================================')
    logging.info('Number of observations: {}'.format(n_obs))
    logging.info('Number of observations in train set: {}'.format(train_split))
    logging.info('Number of observations in test set: {}'.format(test_split))
    logging.info('=====================================')
    dataset = df.values[:, 1:].astype(np.float32)
    data_mean = dataset[:train_split].mean(axis=0)
    data_std = dataset[:train_split].std(axis=0)
    dataset = (dataset - data_mean) / data_std
    dataset = np.nan_to_num(dataset)

    past_history = 5 * 24
    future_target = 6
    step = 1

    logging.info('Past history: {}'.format(past_history))
    logging.info('Future target: {}'.format(future_target))
    logging.info('Step size: {}'.format(step))
    logging.info('=====================================')

    logging.info('Creating train data...')
    x_train, y_train = multivariate_data(dataset, dataset[:, 4], 0, train_split, past_history,
                                         future_target, step, single_step=True)

    logging.info('Creating test data...')
    x_test, y_test = multivariate_data(dataset, dataset[:, 4], train_split, None, past_history,
                                           future_target, step, single_step=True)

    train = {'data': x_train, 'labels': y_train}
    test = {'data': x_test, 'labels': y_test}

    with open(os.path.join(FLAGS.output_folder, 'train.pkl'), 'wb') as f:
        pickle.dump(train, f)

    with open(os.path.join(FLAGS.output_folder, 'test.pkl'), 'wb') as f:
        pickle.dump(test, f)

    logging.info('Dataset created! Lets train!')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
