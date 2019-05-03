import os

import pytest
from meetup_kafka import consumer_writes_data as mk_write
import json


def sampleData():
    data = {'people': []}
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
    return json_data


# testing write function
def test_writeToFile(tmp_path):
    file = tmp_path / 'sub'
    file.mkdir()
    p = file / 'output.txt'
    mk_write.write_data_to_file(p, sampleData())

    with open(p, 'r') as f:
        datastore = json.load(f)

    testData = sampleData()
    assert testData == datastore


# testing chunk of data
def test_chunkOfData():
    datastore = json.loads(sampleData())

    for val in next(mk_write.chunks(datastore['people'], 1)):
        assert val == datastore['people'][0]

# kakfa producer a gonderdigin mesaji, dosyaya yazan ve sonrasinda okuyup karsilastiran bir fonksiyon yazicaksin
# consumer group event icin de yaz bir tane
