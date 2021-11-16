## Imports
import json
import sys
import math
import datetime
import time
import traceback 
from collections import Counter

if __name__ == "__main__":
    weights = []
    buckets = int(sys.argv[1])
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            continue

        data = json.loads(splits[1])
        for item in data:
            if 'weight' in item:
                weights.append(item['weight'])
    
    # sort
    weights.sort()
    total_cnt = len(weights)
    step = 100 / buckets
    for i in range(0, buckets):
        percentile = step * i
        index = percentile * total_cnt / 100
        print '%d\t%d\t%f' % (i, index, weights[index])
