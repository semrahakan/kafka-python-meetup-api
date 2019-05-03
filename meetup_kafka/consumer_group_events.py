import configparser
import json
import datetime
import time
from meetup_kafka import consumer
import logging


def create_kafka_consumer_to_group_event(topic_name:str, server:str, timestamp:str):
    '''

    Uses kafka consumer, and returns a set of different the messages of consumer in every 10 messages with timestamp

    :param topic_name:
    :param server:
    :param timestamp:
    :return:
    '''
    set_of_events = set()
    times = 0
    output_iterator_code = 10
    for msg in consumer.define_consumer(topic_name, server):
        events = msg.value.decode('utf8')
        json_events = json.loads(events)
        try:
            set_of_events.add(json_events["venue"]["venue_id"])
        except Exception as ex:
            pass
        times += 1
        if times == output_iterator_code:
            logging.info(f'Count of distinct events {len(set_of_events)} and time {timestamp}')
            times = 0


if __name__ == '__main__':
    # configuration variables
    config = configparser.ConfigParser()
    path = 'configuration.ini'
    config.read(path)
    server = config['DEFAULT']['server']
    topic_name = config['DEFAULT']['topic_name']
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

    create_kafka_consumer_to_group_event(topic_name, server, st)
