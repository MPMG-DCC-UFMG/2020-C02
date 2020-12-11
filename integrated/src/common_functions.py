import copy
import importlib
import json
import os
import random
import subprocess
import time

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import OffsetAndMetadata

import sys

GROUP_IDS_CANDIDATES = [l.strip() for l in open('../data/config_group_id.csv', 'rt')][1:] if os.path.isfile('../data/config_group_id.csv') else []

DEC = dict([tuple(l.replace('\n', '').split(',')) for l in open('../data/config_mapping.csv', 'rt') if l.strip()][1:])
FIRST_GROUP_NAME_FROM_FILE = GROUP_IDS_CANDIDATES[0] if GROUP_IDS_CANDIDATES else None
# MESSAGES = [str(datetime.datetime.now()), 'Hello,', 'World']
KAFKA_SERVERS = [('hadoopdn-gsi-prod0' + str(j) + '.mpmg.mp.br:6667').replace('010', '10') for j in range(4, 10 + 1)]
KAFKA_SERVERS = KAFKA_SERVERS[:1]
LIBS = {}
# ROWS_INFO_ATOMIC = [ l.strip() for l in open('../data/info_atomic.csv', 'rt') if l.strip() ]
ROWS_INFO_HIGH = [l.strip() for l in open('../data/config_high.csv', 'rt') if l.strip()]
ROWS_INFO_HIGH2ATOMIC = [l.strip() for l in open('../data/config_high2atomic.csv', 'rt') if l.strip()]
ROWS_INFO_TOPICS = [l.strip() for l in open('../data/config_topics.csv', 'rt') if l.strip()]


def get_allowed_social_nets():
    global LIBS
    global ROWS_INFO_TOPICS

    nets = []
    for row in ROWS_INFO_TOPICS:
        net = row.split(',')[0].strip().lower()
        if net != 'network':
            nets.append(net)
            if net not in LIBS:
                LIBS[net] = importlib.import_module(net + '_api')

    return frozenset(nets)


def get_libs():
    global LIBS

    if not LIBS:
        get_allowed_social_nets()

    return LIBS


def get_error_key():
    return 'erro'


def crawl_atomic(atomic_request):
    global LIBS
    global ROWS_INFO_HIGH2ATOMIC

    if atomic_request is None:
        return

    crawling_id = str(int(round(time.time() * 1000)))
    if type(atomic_request) != tuple and type(atomic_request) != list:
        atomic_request = json.loads(atomic_request)

    net, mode, value, which, js = atomic_request
    if net not in LIBS:
        _ = get_allowed_social_nets()

    function_name = None
    for row in ROWS_INFO_HIGH2ATOMIC:
        this_net, this_high_mode, this_atomic, this_respective_function = list(
            map(lambda x: x.strip(), row.strip().lower().split(',')))
        if this_net == net and mode == this_high_mode and which == this_atomic:
            function_name = copy.deepcopy(this_respective_function)

    if function_name is not None:
        # social_media,

        coletor = None
        try:
            coletor = LIBS[net].get_coletor_object(js)
        except Exception as e:
            try:
                coletor = LIBS[net].YoutubeCrawlerAPI(js if type(js) == dict else json.loads(js))
            except Exception as e:
                coletor = LIBS[net].shell(json.dumps(js), mode)
            # coletor = LIBS[net]

        getattr(coletor, function_name)(value, crawling_id)
    else:
        print('not prepared to deal with the following request: %s' % json.dumps(atomic_request))
        exit(0)

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

    ##return _producer
    ### XXX TODO retirar esse return em ambiente producao

    try:
        _producer = KafkaProducer(bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10))
        # if not _producer.bootstrap_connected():
            # _producer = None
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))

    return _producer


def decrypt_string(mstr):
    global DEC

    mstr = str(mstr)
    for pat in DEC:
        if pat in mstr:
            mstr = mstr.replace(pat, DEC[pat])

    return mstr


def systemCommand(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # print(p.stdout.readlines())
    vet = [ l.decode('utf-8').replace('\n', '') for l in p.stdout.readlines() ]
    return vet


def connect_kafka_consumer(topicName):
    '''
    Many thanks to https://www.thebookofjoel.com/python-kafka-consumers
    '''
    global FIRST_GROUP_NAME_FROM_FILE
    global KAFKA_SERVERS

    _consumer = None

    ##return _producer
    ### XXX TODO retirar esse return em ambiente producao
    # _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), enable_auto_commit=False, group_id='ufmg.c02.9997', auto_offset_reset='earliest')

    group_name = copy.deepcopy(FIRST_GROUP_NAME_FROM_FILE) if FIRST_GROUP_NAME_FROM_FILE is not None else 'ufmg.c02.9979'
    # group_name = 'ufmg.c02.9979'
    cmd_output = systemCommand('../../Downloads/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "%s" --describe --group "%s"' % (KAFKA_SERVERS[0], group_name))

    group_exists = 'does not exist' not in str(cmd_output).lower()
    print('group_exists == %s' % str(group_exists))

    # print(group_name, group_exists)

    try:
        _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), auto_offset_reset='earliest', enable_auto_commit=False, group_id=group_name)
        # if group_exists:
            # _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), enable_auto_commit=False, group_id=group_name)
        # else:
            # _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), enable_auto_commit=False, group_id=group_name, auto_offset_reset='earliest')
        # _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), auto_offset_reset='earliest', group_id='ufmg.c02.9999')
        # _consumer = KafkaConsumer(topicName, bootstrap_servers=KAFKA_SERVERS, api_version=(0, 10), auto_offset_reset='earliest')
        # if not _producer.bootstrap_connected():
            # _producer = None
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))

    return group_exists, _consumer


def get_social_network_topic(net):
    global ROWS_INFO_TOPICS

    for row in ROWS_INFO_TOPICS:
        this_net = row.split(',')[0].strip().lower()
        if net == this_net:
            return row.split(',')[1].strip().lower()

    return None


def read_kafka_next_row(topic, close_consumer=True):
    '''
    Many thanks to https://www.thebookofjoel.com/python-kafka-consumers for the reference
    '''

    group_exists, consumer = connect_kafka_consumer(topic)

    # for message in consumer:
        # return message.value.decode('utf-8')

    # return None

    message_batch = consumer.poll(64000)
    # message_batch = consumer.poll(0 if group_exists else 64000)
    # message_batch = consumer.poll(0)
    # if not message_batch:
        # message_batch = consumer.poll(32000)

    for topic_partition, partition_batch in message_batch.items():
        for message in partition_batch:
            # print(message.value.decode('utf-8'))
            # print('>>>>>>', topic_partition, message.offset)
            consumer.commit({topic_partition: OffsetAndMetadata(message.offset+1, "no metadata")})
            if close_consumer:
                consumer.close(0)

            return decrypt_string(message.value.decode('utf-8')) # return iff commit
            # return message.value.decode('utf-8') # return iff commit
            # return message.key.decode('utf-8') + ': ' + decrypt_string(message.value.decode('utf-8')) # return iff commit

    return None


def read_next_atomic_level_from_kafka():
    topics = [ get_social_network_topic(net) for net in get_allowed_social_nets() ]
    random.shuffle(topics)

    # topics = [ t for t in topics if 'insta' in t.lower() ] # XXX
    print(topics)

    for topic in topics:
        mstr = read_kafka_next_row(topic)
        # mstr = None  # read_kafka_next_row(topic)
        if mstr is not None:
            return mstr

    return None


def add_low_level_requests_to_kafka(atomic_requests):
    # atomic_requests.append((net, mode, value, json.dumps(js), 'seguidores'))

    added_ones = []
    producer_instance = connect_kafka_producer()

    for net, mode, value, js, which in atomic_requests:
        if type(js) != dict:
            js = json.loads(js)

        topic = get_social_network_topic(net)
        instance = json.dumps([net, mode, value, which, js])
        print(instance)
        # print(producer_instance)
        # sys.exit(0)
        sent = publish_kafka_message(producer_instance, topic, 'atomic', instance)
        # sent = 1  # XXX publish_kafka_message(producer_instance, topic, 'atomic', instance)
        if sent:
            added_ones.append(json.dumps([net] + json.loads(instance)[:-1]))

    return added_ones


def get_high_level_requests(js, net):
    global ROWS_INFO_HIGH

    high_requests = []

    for row in ROWS_INFO_HIGH:
        this_net, this_mode = row.lower().split(',')
        this_net, this_mode = this_net.strip(), this_mode.strip()

        if this_net == net and this_mode in js:
            already = set()
            for element in js[this_mode]:
                element = element.strip()
                if element not in already:
                    high_requests.append((net, this_mode, element, js))
                    already.add(element)

    return high_requests


def high_to_atomic_level(high_request):
    global ROWS_INFO_HIGH2ATOMIC

    atomic_requests = []

    net, mode, value, js = high_request
    if type(js) != dict:
        js = json.loads(js)

    for row in ROWS_INFO_HIGH2ATOMIC:
        this_net, this_high_mode, this_atomic, _ = list(map(lambda x: x.strip(), row.strip().lower().split(',')))
        if net == this_net and mode == this_high_mode:
            atomic_requests.append((net, mode, value, json.dumps(js), this_atomic))

    return atomic_requests


def has_available_credentials(js, net):
    global LIBS

    if 'tokens' in js or 'chaves_de_acesso' in js:
        this_key = 'tokens' if 'tokens' in js else 'chaves_de_acesso'
        for t in js[this_key]:
            api_object = None
            try:
                api_object = LIBS[net].credentials_to_api_object(t)
            except Exception as e:
                print('cannot build api object due to: %s' % str(e))
            if api_object is not None:
                return True
    else:
        if LIBS[net].needs_credential(js) is False:
            return True

        return LIBS[net].authenticate(js) is not None

    return False


def get_social_net(js):
    if 'coletor' not in js:
        return 'unknown'

    return js['coletor'].strip().lower()


if __name__ == '__main__':
    pass
