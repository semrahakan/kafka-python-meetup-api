import configparser
import json
import datetime
import time
from meetup_kafka import consumer
import logging


def track_of_yes_no_number_rsvps(topic_name:str, server:str, timestamp:str):
    '''

    Uses kafka consumer and returns count of yes/no answers of events with timestamp when count reach 10

    :param topic_name:
    :param server:
    :param timestamp:
    :return:
    '''
    try:
        event_yes = {}
        event_no = {}
        output_iterator_code = 10
        for msg in consumer.define_consumer(topic_name, server):
            events = msg.value.decode('utf8')
            json_events = json.loads(events)
            if json_events["response"] == 'yes':
                event_yes[json_events["rsvp_id"]] = json_events["response"]

            if json_events["response"] == 'no':
                event_no[json_events["rsvp_id"]] = json_events["response"]

            if len(event_yes) % output_iterator_code == 0 or len(event_no) % output_iterator_code == 0:
                logging.info(f'Count of yes {len(event_yes)} and count of no {len(event_no)} time is {timestamp}')


    except Exception as ex:
        logging.error(f'Error occured {ex}')

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
    track_of_yes_no_number_rsvps(topic_name, server, st)
