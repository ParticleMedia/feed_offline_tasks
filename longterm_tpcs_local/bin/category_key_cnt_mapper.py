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

def main():
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'invalid_line')
            continue
        
        key = splits[0]
        cate_list = None
        try:
            cate_list = json.loads(splits[1])
            for cate, info in cate_list.items():
                print '%s\t%s' % (cate, key)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            continue
        hadoop_counter.increase_counter('map', 'pass')
    hadoop_counter.print_counter()

if __name__ == "__main__":
    # Execute Main functionality
    main()
