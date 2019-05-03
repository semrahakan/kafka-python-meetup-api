from kafka import KafkaConsumer

def define_consumer(topic_name: str, server):
    '''

    Defines kafka consumer

    :param topic_name:
    :param server:
    :return:
    '''
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=[server])
    return consumer