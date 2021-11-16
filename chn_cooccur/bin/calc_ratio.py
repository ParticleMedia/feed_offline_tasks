## Spark Application - execute with spark-submit

## Imports
import json
import sys
import math
import datetime
import time
import traceback 
from collections import Counter
import utils
hadoop_counter = utils.Counter() 


def main():
    for line in sys.stdin:
        line = line.strip()
        hadoop_counter.increase_counter('count', 'lines')
        if len(line) == 0:
            continue
        
        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid click line: %s\n' % line)
            continue

        if line.find("grp#chn#") == -1:
            hadoop_counter.increase_counter("count", 'no_chn')
            continue

        if (len(splits[0].split('#')) > 3) and "android" in splits[0].split('#')[3]:
            chn = splits[0].split('#')[2]
        else:
            hadoop_counter.increase_counter("count", 'no_chn')
            continue
        cate_data = None
        try:
            cate_data = json.loads(splits[1])
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            continue

        neg_c = int(cate_data['neg'].split(" ")[1])
        neg_v = int(cate_data['neg'].split(" ")[0])
        pos_c = int(cate_data['pos'].split(" ")[1])
        pos_v = int(cate_data['pos'].split(" ")[0])
        no_feed_c = int(cate_data['nofeedback'].split(" ")[1])
        no_feed_v = int(cate_data['nofeedback'].split(" ")[0])
        if no_feed_v == 0 or no_feed_c == 0:
            print('%s\t%f\t%f' % (chn, -1.0, -1.0))
            continue
        if pos_v == 0:
            pos_r = -1.0
        else:
            pos_r = 1.0*pos_c/pos_v*no_feed_v/no_feed_c

        if neg_v == 0:
            neg_r = -1.0
        else:
            neg_r = 1.0*neg_c/neg_v*no_feed_v/no_feed_c

        print("%s\t%f\t%f" % (chn, pos_r, neg_r))

if __name__ == "__main__":
    main()
    hadoop_counter.print_counter()
