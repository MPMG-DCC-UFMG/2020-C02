import json
import twitter_api

from kafka import KafkaConsumer
from kafka import KafkaProducer


# MESSAGES = [ str(datetime.datetime.now()), 'Hello,', 'World' ]
KAFKA_SERVERS = [ ('hadoopdn-gsi-prod0' + str(j) + '.mpmg.mp.br:6667').replace('010', '10') for j in range(4, 10+1) ]
KAFKA_SERVERS = KAFKA_SERVERS[:1]


def get_allowed_social_nets():
    return frozenset([ 'twitter' ])


def get_error_key():
    return 'erro'


def crawl_atomic(atomic_request):
    if type(atomic_request) != tuple and type(atomic_request) != list:
        atomic_request = json.loads(atomic_request)

    net, container, value, js, which = atomic_request

    finished = False
    if net == 'twitter':
        if which == 'posts':
            twitter_api.download_posts()
            finished = True
        elif which == 'perfil':
            XXX
            finished = True
        elif which == 'seguidores':
            XXX
            finished = True
        elif which == 'seguindo':
            XXX
            finished = True

    if not finished:
        print('not prepared to deal with the following request: %s' % json.dumps(atomic_request))

    return
   


def publish_kafka_message(producer_instance, topic_name, key, value):
    '''
    Many thanks to https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
    '''

    # usage example:
    # publish_message(kafka_producer, 'crawler_twitter_post', 'raw', my_string.strip())
    sent = False

    if producer_instance is not None:
        try:
            key_bytes = bytes(key, encoding='utf-8')
            value_bytes = bytes(value, encoding='utf-8')
            producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
            producer_instance.flush()
            print('Message published successfully.')
            sent = True
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))

    return sent


def connect_kafka_producer():
    global KAFKA_SERVERS

    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10))
        if not _producer.bootstrap_connected():
            _producer = None
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))

    return _producer


def get_social_network_topic(net):
    if net == 'twitter':
        return 'crawler_twitter'

    return None


def read_next_atomic_level_from_kafka():
    XXX = YYY

    topics = [ 'crawler_twitter', 'crawler_youtube' ]
    for topic in topics:
        mstr = read_kafka_next_row(topic)
        if mstr is not None:
            return mstr

    return None


def add_low_level_requests_to_kafka(atomic_requests):
    # atomic_requests.append((net, container, value, json.dumps(js), 'seguidores'))

    added_ones = []
    producer_instance = connect_kafka_producer()

    print(atomic_requests[0])
    for net, container, value, js, which in atomic_requests:
        if type(js) != dict:
            js = json.loads(js)

        topic = get_social_network_topic(net)
        instance = json.dumps([ container, value, which, js ])
        sent = publish_kafka_message(producer_instance, topic, 'raw', 'test msg XXX')
        sent = 1 # XXX
        if sent:
            added_ones.append(json.dumps([ net ] + json.loads(instance)[:-1]))

    return added_ones


def get_high_level_requests(js, net):
    high_requests = []

    if net == 'twitter':
        for container in [ 'users', 'words' ]:
            already = set()
            if container in js:
                for element in js[container]:
                    element = element.strip()
                    if element not in already:
                        high_requests.append(( net, container, element, js ))
                        already.add(element)

    return high_requests


def high_to_atomic_level(high_request):
    atomic_requests = []

    net, container, value, js = high_request
    if type(js) != dict:
        js = json.loads(js)

    if net == 'twitter':
        if container in [ 'users', 'usuarios' ]:
            atomic_requests.append((net, container, value, json.dumps(js), 'perfil'))
            atomic_requests.append((net, container, value, json.dumps(js), 'seguidores'))
            atomic_requests.append((net, container, value, json.dumps(js), 'seguindo'))
            atomic_requests.append((net, container, value, json.dumps(js), 'posts'))
        elif container in [ 'words', 'keywords', 'palavras' ]:
            atomic_requests.append((net, container, value, json.dumps(js), 'posts'))

    return atomic_requests


def has_available_credentials(js, net):
    if net == 'twitter':
        if 'tokens' in js:
            for t in js['tokens']:
                api_object = None
                try:
                    api_object = twitter_api.credentials_to_api_object(t)
                except:
                    pass
                if api_object is not None:
                    return True

        return False

    return False


def get_social_net(js):
    if 'coletor' not in js:
        return 'unknown'

    return js['coletor'].strip().lower()


if __name__ == '__main__':
    pass
