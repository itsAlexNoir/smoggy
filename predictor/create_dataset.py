import os
import numpy as np
import pandas as pd
from absl import app, flags
import logging

from ptools import plotting


def define_flags():
    flags.DEFINE_string(name='source_path', default=None, help='Path to the source of the data')


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
    logging.info(' '*20 + 'Create dataset')
    logging.info('=' * 80 + '\n')

    univariate = True
    df = pd.read_pickle(os.path.join(FLAGS.source_path, 'air_df.pkl'))

    if univariate:
        df = df[df['station'] == 4]
        df = df[df['magnitud'] == 8]

        n_obs = len(df.index)
        TEST_SPLIT = int(n_obs * 0.15)
        TRAIN_SPLIT = n_obs - TEST_SPLIT
        uni_data = df['value']
        uni_data.index = df['date']

        uni_data = uni_data.values
        uni_mean = uni_data[:TRAIN_SPLIT].mean()
        uni_std = uni_data[:TRAIN_SPLIT].std()
        uni_data = (uni_data - uni_mean) / uni_std

        univariate_past_history = 10
        univariate_future_target = 0

        x_train_uni, y_train_uni = univariate_data(uni_data, 0, TRAIN_SPLIT,
                                                   univariate_past_history,
                                                   univariate_future_target)
        x_val_uni, y_val_uni = univariate_data(uni_data, TRAIN_SPLIT, None,
                                               univariate_past_history,
                                               univariate_future_target)



    logging.info('Dataset created! Lets train dude!')


if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
