#input:  uid \t docid \t ts \t docInfo \t clicked ...
#output: category \t ts \t chekced \t clicked ...

import sys
sys.path.insert(0, '')
import math
import datetime
import time
import traceback
import json
import utils
import action

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

def main(min_ts, field):
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

            docInfo = json.loads(splits[3])
            if field not in docInfo or len(docInfo.get(field, [])) == 0:
                hadoop_counter.increase_counter('map', 'no_category')
                continue

            actionSplits = ['1']
            actionSplits.extend(splits[4:])
            act = action.parseAction(actionSplits)
            act.normalize()
            if not act.isValidCheck():
                hadoop_counter.increase_counter('map', 'filter_by_cvtime')
                continue
            
            actList = act.toList()
            for v in docInfo[field]:
                try:
                    out = [v, splits[2]]
                    out.extend(actList)
                    print '\t'.join(out)
                except Exception as e:
                    traceback.print_exc()
                    sys.stderr.write('invalid value: %s\n' % v)
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'parse_line_fail')
    hadoop_counter.print_counter()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s filter_days field\n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    filter_days = int(sys.argv[1])
    field = sys.argv[2]
    min_ts = getTimestampOfToday() - 3600 * 24 * filter_days
    sys.stderr.write('min ts: %d\n' % min_ts)
    main(min_ts, field)
