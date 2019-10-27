import os
import numpy as np
import pandas as pd
from absl import app, flags
import logging

from ptools import plotting


def define_flags():
    flags.DEFINE_string(name='source_file', default=None, help='Path to the source of the data')
    flags.DEFINE_bool(name='univariate', default=True, help='Choose if create an univariate dataset')


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


def main(argv):
    logging.info('='*80)
    logging.info(' ' * 20 + 'Create dataset')
    logging.info('=' * 80 + '\n')

    df = pd.read_pickle(FLAGS.source_file)
    if FLAGS.univariate:
        df = df[df['magnitud'] == 8]

        n_obs = len(df.index)
        test_split = int(n_obs * 0.15)
        train_split = n_obs - test_split
        uni_data = df['value']
        uni_data.index = df['date']

        uni_data = uni_data.values
        uni_mean = uni_data[:train_split].mean()
        uni_std = uni_data[:train_split].std()
        uni_data = (uni_data - uni_mean) / uni_std

        univariate_past_history = 10
        univariate_future_target = 0

        x_train_uni, y_train_uni = univariate_data(uni_data, 0, train_split,
                                                   univariate_past_history,
                                                   univariate_future_target)
        x_val_uni, y_val_uni = univariate_data(uni_data, train_split, None,
                                               univariate_past_history,
                                               univariate_future_target)



    logging.info('Dataset created! Lets train dude!')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
