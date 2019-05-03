from kafka import KafkaProducer
import configparser
import requests
import logging

def generate_data(request_url: str)->bytes:
    '''

    Uses requests module to get items of an url, returns a generator contains elements of the url

    :param request_url:
    :return:
    '''
    try:
        rq = requests.get(request_url, stream=True)
        if rq.status_code == 200:
            for chunk in rq.iter_lines():
                yield chunk
    except Exception as ex:
        logging.error(f'Connection to meetup data could not established, {ex}')

def publish_message(producer_instance, topic_name: str, request_url: str)->None:
    '''
    Uses kafka producer to send messages

    :param producer_instance:
    :param topic_name:
    :param request_url:
    :return:
    '''
    try:
        data = generate_data(request_url)
        for item in data:
            producer_instance.send(topic_name, item)
            logging.info('Message published successfully.')
    except Exception as ex:
        logging.error(f'Message could not sent, {ex}')


def connect_kafka_producer(server_con)->KafkaProducer:
    '''

    Connects kafka producer to given server

    :param server_con:
    :return:
    '''

    producer = KafkaProducer(bootstrap_servers=[server_con])
    logging.info('Producer created successfully')
    return producer
    '''
    
    try:
        _producer = KafkaProducer(bootstrap_servers=[server_con])
        logging.info('Producer created successfully')
    except Exception as ex:
        logging.error(f'Exception while connecting Kafka, {str(ex)}')
    '''


if __name__ == '__main__':
    config = configparser.ConfigParser()
    path = 'configuration.ini'
    config.read(path)

    server = config['DEFAULT']['server']
    topic_name = config['DEFAULT']['topic_name']
    request_url = config['DEFAULT']['request_url']
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    producer = connect_kafka_producer(server)
    if producer:
        publish_message(producer, topic_name, request_url)