import copy
import crawler.api as api
import json
import os
import time

from sys import argv


target_json_folder = '/var/twitter-crawler/jsons/'
if not os.path.isdir(target_json_folder):
    os.mkdir(target_json_folder)
target_json_filename = target_json_folder + str(int(time.time()*1000)) + '.json'
target_json = None
    
if not (len(argv) == 2 or (len(argv) == 3 and '-d' in argv)):
    target_json = json.dumps({ 'error': 'Usage: "python3 main.py <input-json>" or "python3 main.py -d <input-json-dump>"' })
else:
    try:
        Json = json.dumps(json.loads(argv[argv.index('-d')+1]))
    except:
        try:
            with open(target_json_folder + '../' + argv[1].split('/')[-1], 'rt') as f:
                Json = json.dumps(json.load(f))
        except:
            target_json = json.dumps({ 'error': 'Usage: "python3 main.py <input-json>" or "python3 main.py -d <input-json-dump>"' })


    if 'error' in json.loads(Json):
        target_json = copy.deepcopy(Json)
    else:
        error_Json = { 'error': [] }
        joining_timestamps = []


        for kind in [ 'users', 'keywords' ]:
            timestamp = None

            try:
                tt = api.shell(Json, kind)
                timestamp = tt.start(verbose=False)
                if int(timestamp):
                    joining_timestamps.append(int(timestamp))
            except Exception as e:
                this_message = 'error getting tweets: ' + str(e)
                if this_message not in error_Json['error']:
                    error_Json['error'].append(this_message)


        if error_Json['error']:
            error_Json['error'] = ' ; '.join(error_Json['error'])
            target_json = json.dumps(error_Json)
        else:
            target_json = json.dumps(api.shell(Json, 'users').joined_timestamps(joining_timestamps))


with open(target_json_filename, 'wt+') as fw:
    fw.write(target_json + '\n')
print(target_json)
