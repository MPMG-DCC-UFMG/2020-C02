import datetime

from kafka import KafkaConsumer
from kafka import KafkaProducer


MESSAGES = [ str(datetime.datetime.now()), 'Hello,', 'World' ]
SERVERS = [ ('hadoopdn-gsi-prod0' + str(j) + '.mpmg.mp.br:6667').replace('010', '10') for j in range(4, 10+1) ][:1]


def get_recipes():
    global MESSAGES

    return MESSAGES


def publish_message(producer_instance, topic_name, key, value):
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


if __name__ == '__main__':
    '''
    Many thaks to https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
    '''

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    all_recipes = get_recipes()
    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        if kafka_producer is not None:
            for recipe in all_recipes:
                publish_message(kafka_producer, 'crawler_twitter_post', 'raw', recipe.strip())
            kafka_producer.close()
