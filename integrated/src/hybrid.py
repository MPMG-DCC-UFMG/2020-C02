import common_functions as common
import json
import sys
import time


SLEEPING_SECONDS = 8


if __name__ == '__main__':
    '''
    while True:
        read_atomic_level_requests()
        read_from_kafka_and_run_a_single_atomic_level_request()
    '''

    if '-j' in sys.argv:
        atomic_level_request = json.loads([ l.strip() for l in open('../data/my_atomic_instance.txt', 'rt') if l.strip() ][0])
        common.crawl_atomic(atomic_level_request)
    else:
        while True:
            atomic_level_request = common.read_next_atomic_level_from_kafka()
            if atomic_level_request is not None:
                common.crawl_atomic(atomic_level_request)
            
            time.sleep(SLEEPING_SECONDS)
