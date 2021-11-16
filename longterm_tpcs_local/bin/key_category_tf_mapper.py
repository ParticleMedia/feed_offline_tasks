import json
import sys
sys.path.insert(0, '')
import math
import datetime
import time
import traceback 
import utils

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

def main(key, min_ts):
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        
        try:
            clickInfo = json.loads(line)
            if 'tpc_s' not in clickInfo:
                hadoop_counter.increase_counter('map', 'no_tpcs')
                continue
            
            if key not in clickInfo or len(clickInfo[key]) == 0:
                hadoop_counter.increase_counter('map', 'no_key')
                continue
            
            ts = clickInfo.get('ts', 0)
            if ts <= min_ts:
                hadoop_counter.increase_counter('map', 'filter_by_ts')
                continue
            
            if clickInfo.get('pv_time', 0) < 2000:
                hadoop_counter.increase_counter('map', 'filter_by_pv_time')
                continue

            keyStr = clickInfo[key]
            print '%s\t%s' % (keyStr, line)
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('map', 'load_json_fail')
    hadoop_counter.print_counter()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s key filter_days\n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    key = sys.argv[1]
    filter_days = int(sys.argv[2])
    min_ts = getTimestampOfToday() - 3600 * 24 * filter_days
    sys.stderr.write('min ts: %d\n' % min_ts)
    main(key, min_ts)
