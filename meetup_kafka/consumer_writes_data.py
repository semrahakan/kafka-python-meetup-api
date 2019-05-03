import configparser
import itertools
import json
import datetime

from kafka.consumer.fetcher import ConsumerRecord

from meetup_kafka import consumer
import logging

def chunks(iterable, size:int)->ConsumerRecord:
    '''

    Takes an iterable, and returns given size of iterable

   # >>> chunks('{'person': {'name':'semra', 'age': '26'}, {'name':'hakan', 'age': '27'}}', 2)
    #json_sample

    :param iterable:
    :param size:
    :return:
    '''
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))

def write_data_to_file(file_name: str, msg_from_kafka):
    '''

    Takes file name and an item to be written in this file.

    :param file_name:
    :param msg_from_kafka:
    :return:
    '''
    try:
        with open(file_name, 'w') as fd:
            json.dump(msg_from_kafka, fd, indent=4, sort_keys=True)
            logging.info('Data is being written')
    except Exception as e:
        logging.error(f'Cannot write to the file,: {e}')

if __name__ == '__main__':
    #configuration variables
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    config = configparser.ConfigParser()
    path = 'configuration.ini'
    config.read(path)
    file_name = 'files/' + config['DEFAULT']['file_name']
    server = config['DEFAULT']['server']
    topic_name = config['DEFAULT']['topic_name']
    request_url = config['DEFAULT']['request_url']
    size = int(config['DEFAULT']['size'])

    time_stamp = datetime.datetime.now().timestamp()

    msg_from_kafka = consumer.define_consumer(topic_name, server)
    file_name = file_name + str(time_stamp)

    data = list()
    #brings messages according to amount of size
    for val in next(chunks(msg_from_kafka, size)):
        events = val.value.decode('utf8')
        data.append(json.loads(events))
        logging.info(data)
    write_data_to_file(file_name, data)

