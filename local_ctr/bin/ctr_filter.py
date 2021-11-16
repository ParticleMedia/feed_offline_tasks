import sys
sys.path.insert(0, '')
import json
import traceback
import time

import utils

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

if __name__ == "__main__":
    hadoop_counter = utils.Counter()
    pivot_days = int(sys.argv[1])
    min_ts = getTimestampOfToday() - 3600 * 24 * pivot_days
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('filter', 'invalid_line')
            continue

        ts = int(splits[2])
        if ts < min_ts:
            hadoop_counter.increase_counter('filter', 'filter')
            continue
        else:
            try:
                data = json.loads(splits[1])
                text_category = data.get('text_category', {})
                if 'CrimePublicsafety' in text_category.get('first_cat', {}):
                    hadoop_counter.increase_counter('filter', 'crime')
                    continue
                print '%s\t%s' % (splits[0], splits[1])
                hadoop_counter.increase_counter('filter', 'pass')
            except Exception as e:
                traceback.print_exc()
                sys.stderr.write('load doc json failed: %s\n' % splits[1])
                hadoop_counter.increase_counter('join', 'invalid_json')
    hadoop_counter.print_counter()
