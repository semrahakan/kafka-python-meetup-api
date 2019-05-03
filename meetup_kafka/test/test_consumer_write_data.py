from meetup_kafka import producer as pr
from meetup_kafka import consumer as cr
from meetup_kafka import consumer_writes_data as mk_write
import pytest
import configparser
import json


@pytest.fixture
def server():
    return 'localhost:9092'

@pytest.fixture
def producer(server):
    return pr.connect_kafka_producer(server)

@pytest.fixture
def topic():
    return 'meetup_v3'

def sample_json():
    data = {}
    data['people'] = []
    data['people'].append({
        'name': 'Scott',
        'website': 'stackabuse.com',
        'from': 'Nebraska'
    })

    data['people'].append({
        'name': 'Larry',
        'website': 'google.com',
        'from': 'Michigan'
    })

    json_data = json.dumps(data)
    return json_data.encode()


# send json file to producer
def producer_to_send (producer, topic):
    key = b'people'
    value = sample_json()
    producer.send(topic, key=key, value=value)
    producer.flush()


def test_consumer_write_data(tmp_path, producer, topic, server):
    producer_to_send(producer, topic)

    file = tmp_path / 'sub'
    file.mkdir()
    p = file / 'output.txt'

    consumer = cr.define_consumer(topic, server)
    msg = next(consumer)
    mk_write.write_data_to_file(p, msg)

    with open(p, 'r') as f:
        datastore = json.load(f)
    assert datastore == msg.value