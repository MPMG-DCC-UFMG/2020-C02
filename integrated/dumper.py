import copy
import json
import os
import sys


if __name__ == '__main__':
    filename = copy.deepcopy(sys.argv[1])
    try:
        filename = copy.deepcopy(sys.argv[1])
    except:
        pass

    answer = 'file does not exist :('
    if os.path.isfile(filename):
        with open(filename, 'rt') as f:
            answer = json.dumps(json.load(f))

    print(answer)
