import os
import numpy as np
import tensorflow as tf
from absl import app, flags
import logging
import pickle


def define_flags():
    flags.DEFINE_string(name='source_path', default=None, help='Path to the source of the data')
    flags.DEFINE_string(name='output_folder', default=None, help='Output folder for dataset')
    flags.DEFINE_integer(name='batch_size', default=32, help='Batch size')
    flags.DEFINE_integer(name='buffer_size', default=1000, help='Buffer size for the shuffle')


def load_dataset(path, BATCH_SIZE, BUFFER_SIZE):
    with open(os.path.join(path, 'train.pkl'), 'rb') as f:
        train = pickle.load(f)

    with open(os.path.join(path, 'test.pkl'), 'rb') as f:
        test = pickle.load(f)

    train_set = tf.data.Dataset.from_tensor_slices((train['data'], train['labels']))
    train_set = train_set.cache().shuffle(BUFFER_SIZE).batch(BATCH_SIZE).repeat()

    test_set = tf.data.Dataset.from_tensor_slices((test['data'], test['labels']))
    test_set = test_set.batch(BATCH_SIZE).repeat()

    return train_set, test_set

def main(argv):
    logging.info('='*80)
    logging.info(' ' * 20 + 'Create dataset')
    logging.info('=' * 80 + '\n')

    logging.info('Loading dataset from source...')
    train_set, test_set = load_dataset(FLAGS.source_path, FLAGS.batch_size, FLAGS.buffer_size)

    with open(os.path.join(FLAGS.source_path, 'train.pkl'), 'rb') as f:
        train = pickle.load(f)

    single_step_model = tf.keras.models.Sequential()
    single_step_model.add(tf.keras.layers.LSTM(32, input_shape=train['data'].shape[-2:]))
    single_step_model.add(tf.keras.layers.Dense(1))

    single_step_model.compile(optimizer=tf.keras.optimizers.RMSprop(), loss='mae')

    EVALUATION_INTERVAL = 200
    EPOCHS = 10

    single_step_history = single_step_model.fit(train_set, epochs=EPOCHS,
                                                steps_per_epoch=EVALUATION_INTERVAL,
                                                validation_data=test_set,
                                                validation_steps=50)
    logging.info('=' * 60)
    logging.info('Training finished!')
    logging.info('='*60)

if __name__ == '__main__':
    FLAGS = flags.FLAGS
    define_flags()
    app.run(main)
