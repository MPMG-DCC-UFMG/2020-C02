import common_functions as common
import copy
import json
import random
import sys


LIBS = common.get_libs()
LOGGING = True


def read_input_json():
    argv = copy.deepcopy(sys.argv)
    js = json.loads(argv[argv.index('-d')+1])

    return js


if __name__ == '__main__':
    '''
    js = read_input_json()
    assert(ready_to_social_network(js))
    assert(credential_is_ok(js))
    high_level_requests = get_high_level_requests(js)
    assert(len(high_level_requests))
    atomic_level_requests = break_high_level_requests_into_minors()
    append@kafka(atomic_level_requests)
    print_to_user(high_level_requests)
    '''
    
    js = read_input_json()
    feedback_js = {}
    high_level_requets = {}

    if common.get_error_key() not in js:
        this_social_network = common.get_social_net(js)
        if this_social_network == 'twitter':
            js = LIBS['twitter'].translate_json_keys(js, 'pt', 'en', recursively=True)

        if this_social_network not in common.get_allowed_social_nets():
            feedback_js = { 'status': '%s: not prepared to deal with "%s" social network' % (common.get_error_key(), this_social_network) }
        else:
            if not common.has_available_credentials(json.loads(common.decrypt_string(json.dumps(js))), this_social_network):
                feedback_js = { 'status': '%s: %s' % (common.get_error_key(), 'at least one valid credential must be assigned in order to continue') }
            else:
                high_level_requests = common.get_high_level_requests(js, this_social_network)
                if high_level_requests:
                    atomic_requests = []
                    for high_level_request in high_level_requests:
                        atomic_requests.extend(common.high_to_atomic_level(high_level_request))
                    random.shuffle(atomic_requests)
                    
                    added_atomic = common.add_low_level_requests_to_kafka(atomic_requests)
                    if added_atomic:
                        feedback_js = { 'status': 'the following requests are successfully added to queue: %s' % (', '.join(added_atomic)) }
                    else:
                        feedback_js = { 'status': 'there is no request successfully created' }
    else:
        feedback_js = { 'status': '%s: %s' % (common.get_error_key(), js[common.get_error_key()]) }

    print(feedback_js)
