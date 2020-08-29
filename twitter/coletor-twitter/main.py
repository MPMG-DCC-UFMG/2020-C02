import copy
import crawler.api as api
import json
import os
import time

from sys import argv


error_key = 'erro'


target_json_folder = '/var/twitter-crawler/jsons/'
if not os.path.isdir(target_json_folder):
    os.mkdir(target_json_folder)
target_json_filename = target_json_folder + str(int(time.time()*1000)) + '.json'
target_json = None
    
if not (len(argv) == 2 or (len(argv) == 3 and '-d' in argv)):
    target_json = json.dumps({ error_key: 'Usage: "python3 main.py <input-json>" or "python3 main.py -d <input-json-dump>"' })
else:
    try:
        Json = json.dumps(json.loads(argv[argv.index('-d')+1]))
    except:
        try:
            with open(target_json_folder + '../' + argv[1].split('/')[-1], 'rt') as f:
                Json = json.dumps(json.load(f))
        except:
            target_json = json.dumps({ error_key: 'Usage: "python3 main.py <input-json>" or "python3 main.py -d <input-json-dump>"' })

    # joining_timestamps = [ '1598620735908' ]
    # target_json = json.dumps(api.shell(Json, 'users').joined_timestamps(joining_timestamps))
    # print(json.loads(target_json))

    if error_key in json.loads(Json):
        target_json = copy.deepcopy(Json)
    else:
        if 'usuarios' in Json or 'palavras' in Json:
            Json = json.dumps(api.translate_json_keys(json.loads(Json), 'pt', 'en', recursively=True))

        error_Json = { error_key: [] }
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
                if this_message not in error_Json[error_key]:
                    error_Json[error_key].append(this_message)


        if error_Json[error_key]:
            error_Json[error_key] = ' ; '.join(error_Json[error_key])
            target_json = json.dumps(error_Json)
        else:
            target_json = json.dumps(api.shell(Json, 'users').joined_timestamps(joining_timestamps))


with open(target_json_filename, 'wt+') as fw:
    fw.write(target_json + '\n')
print(target_json)
