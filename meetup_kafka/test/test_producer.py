from meetup_kafka import producer as pr
from meetup_kafka import consumer as cr
import pytest
import configparser

@pytest.fixture
def topic():
    return 'meetup_v9'

@pytest.fixture
def server():
    return 'localhost:9092'

@pytest.fixture
def producer(server):
    return pr.connect_kafka_producer(server)


def test_send_message_to_kafka(producer, topic, server):

    key = b'foo'
    value = b'bar3'
    #topic = 'meetup_v6'
    producer.send(topic, key=key, value=value)
    producer.flush()

    consumer = cr.define_consumer(topic, server)
    #msg = next(consumer)
    for msg in consumer:
        assert msg.key == key and msg.value == value
