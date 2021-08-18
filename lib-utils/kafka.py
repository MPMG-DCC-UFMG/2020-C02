'''
Many thanks to https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
'''

import datetime

from kafka import KafkaConsumer
from kafka import KafkaProducer


# MESSAGES = [ str(datetime.datetime.now()), 'Hello,', 'World' ]
SERVERS = [ ('hadoopdn-gsi-prod0' + str(j) + '.mpmg.mp.br:6667').replace('010', '10') for j in range(4, 10+1) ]
SERVERS = SERVERS[:1]


def publish_message(producer_instance, topic_name, key, value):
    # usage example:
    # publish_message(kafka_producer, 'crawler_twitter_post', 'raw', my_string.strip())

    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    global SERVERS

    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=SERVERS, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer
