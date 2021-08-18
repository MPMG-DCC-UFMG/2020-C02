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
        atomic_key, atomic_level_request = common.read_next_atomic_level_from_kafka(get_key=True)
        # atomic_level_request = json.loads([ l.strip() for l in open('../data/my_atomic_instance.txt', 'rt') if l.strip() ][0])
        # atomic_level_request = None
        # print(json.dumps(json.loads(atomic_level_request)[:-1]))

        if atomic_level_request is not None:
            common.update_atomic_request_status(json.dumps([ atomic_key ] + json.loads(atomic_level_request)), 'running')
            error_string = ''
            try:
                common.crawl_atomic(atomic_level_request)
            except Exception as e:
                error_string = '%s' % str(e)
                print(error_string)
            final_status = 'finished: success' if not error_string else 'finished: failure -- %s' % error_string
            common.update_atomic_request_status(json.dumps([ atomic_key ] + json.loads(atomic_level_request)), final_status)

    elif '--empty' in sys.argv:
        atomic_level_request = ''
        while atomic_level_request is not None:
            atomic_key, atomic_level_request = common.read_next_atomic_level_from_kafka(get_key=True)
            print(atomic_level_request)

    else:
        while True:
            atomic_key, atomic_level_request = common.read_next_atomic_level_from_kafka(get_key=True)
            print(common.magenta_string(str(atomic_level_request)))
            if atomic_level_request is not None:
                common.update_atomic_request_status(json.dumps([ atomic_key ] + json.loads(atomic_level_request)), 'running')
                error_string = ''
                try:
                    common.crawl_atomic(atomic_level_request)
                except Exception as e:
                    error_string = '%s' % str(e)
                    print(error_string)
                final_status = 'finished: success' if not error_string else 'finished: failure -- %s' % error_string
                common.update_atomic_request_status(json.dumps([ atomic_key ] + json.loads(atomic_level_request)), final_status)
            
            time.sleep(SLEEPING_SECONDS)
