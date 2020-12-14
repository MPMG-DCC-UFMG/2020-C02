import common_functions as common
import json
import os
import sys
import time


TIMEOUT_WAITING_CANDIDATES = [float(l.strip().split(',')[-1]) for l in open('../data/config_timeouts.csv', 'rt') if 'between_timeout' in l.lower()] if os.path.isfile('../data/config_timeouts.csv') else []
SLEEPING_SECONDS = TIMEOUT_WAITING_CANDIDATES[0] if TIMEOUT_WAITING_CANDIDATES else 8


if __name__ == '__main__':
    '''
    while True:
        read_atomic_level_requests()
        read_from_kafka_and_run_a_single_atomic_level_request()
    '''

    # for j in range(15):
        # atomic_level_request = common.read_next_atomic_level_from_kafka()
        # print(j, atomic_level_request)
        # print()
    # exit(0)
    
    if '-j' in sys.argv:
        atomic_level_request = common.read_next_atomic_level_from_kafka()
        # atomic_level_request = json.loads([ l.strip() for l in open('../data/my_atomic_instance.txt', 'rt') if l.strip() ][0])
        # atomic_level_request = None
        print(json.dumps(json.loads(atomic_level_request)[:-1]))

        if atomic_level_request is not None:
            common.crawl_atomic(atomic_level_request)
    elif '--empty' in sys.argv:
        atomic_level_request = ''
        while atomic_level_request is not None:
            atomic_level_request = common.read_next_atomic_level_from_kafka()
            print(atomic_level_request)
    else:
        while True:
            atomic_level_request = common.read_next_atomic_level_from_kafka()
            if atomic_level_request is not None:
                try:
                    common.crawl_atomic(atomic_level_request)
                except Exception as e:
                    print('crawling failed: %s' % str(e))
            
            time.sleep(SLEEPING_SECONDS)
