#input:  uid \t docid \t ts \t clicked \t text_category
#output: uid \t docid \t ts \t clicked \t text_category

import sys
sys.path.insert(0, '')
import math
import datetime
import time
import traceback
import json
import utils

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

def main(min_ts):
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        
        splits = line.split('\t')
        if len(splits) < 5:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue

        try:
            ts = int(splits[2])
            if ts <= min_ts:
                hadoop_counter.increase_counter('map', 'filter_by_ts')
                continue
            if len(splits[4]) <= 2:
                hadoop_counter.increase_counter('map', 'no_category')
                continue
            print line
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'parse_line_fail')
    hadoop_counter.print_counter()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage python %s filter_days\n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    filter_days = int(sys.argv[1])
    min_ts = getTimestampOfToday() - 3600 * 24 * filter_days
    sys.stderr.write('min ts: %d\n' % min_ts)
    main(min_ts)
