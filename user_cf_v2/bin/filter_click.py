import sys
sys.path.insert(0, '')
import json
import traceback
import time
import math
import utils

PIVOT_DAYS = 90

if __name__ == "__main__":
    pivot = time.time() - 3600 * 24 * PIVOT_DAYS
    sys.stderr.write('pivot: %d\n' % pivot)
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('filter', 'invalid_line')
            continue

        try:
            uid = splits[0]
            data = json.loads(splits[1])

            ts = data.get('ts', 0)
            pv_time = data.get('pv_time', 0)
            if ts < pivot or pv_time < 2000:
                hadoop_counter.increase_counter('filter', 'filter')
                continue
            print line
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('filter', 'invalid_json')

