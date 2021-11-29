import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
import math

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        info = json.loads(line.split('\t')[1])
        print 'clu_%d,%d,%f' % (info['id'], info['user'], float(info['user']) / float(477701))
