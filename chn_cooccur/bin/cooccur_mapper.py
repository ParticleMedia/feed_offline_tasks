## Spark Application - execute with spark-submit

## Imports
import json
import sys
import math
import datetime
import time
import traceback 
from collections import Counter

def load_category_cnt(category_cnt_file):
    cate_cnt = {}
    with open(category_cnt_file) as cate_cnt_fd:
        for line in cate_cnt_fd:
            line = line.strip()
            if len(line) == 0:
                continue
            splits = line.split('\t')
            if len(splits) != 2:
                sys.stderr.write('invalid catetory line: %s\n' % line)
                continue

            category = splits[0]
            cnt = float(splits[1])
            cate_cnt[category] = cnt
            #sys.stderr.write('category %s cnt %f\n' % (category, cnt))
    sys.stderr.write('load category cnt: %s\n' % len(cate_cnt))
    return cate_cnt

def main(category_cnt_file):
    cate_cnt_dict = load_category_cnt(category_cnt_file)
    if cate_cnt_dict is None or len(cate_cnt_dict) == 0:
        sys.stderr.write('failed to load category idf from: %s\n' % category_cnt_file)
        sys.exit(1)

    line_cnt = 0
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        line_cnt += 1
        if line_cnt % 100000 == 0:
            sys.stderr.write('processed click lines: %d\n' % line_cnt)

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid click line: %s\n' % line)
            continue

        key = splits[0]
        cate_data = None
        try:
            cate_data = json.loads(splits[1])
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            continue

        for cate1, v1 in cate_data.items():
            for cate2, v2 in cate_data.items():
                if (cate2 != cate1):
                    cnt1 = cate_cnt_dict.get(cate1, 0.0)
                    cnt2 = cate_cnt_dict.get(cate2, 0.0)
                    if (float(cnt1) == 0.0 or float(cnt2) == 0.0):
                        continue
                    cooccur_score = (float(v1['tf']) + float(v2['tf'])) / math.sqrt(float(cnt1) * float(cnt2))
                    print('\t'.join([cate1,cate2, '%.6f' % cooccur_score]))
    sys.stderr.write("parse click finished\n")

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage python %s category_cnt_file \n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    category_cnt_file = sys.argv[1]
    main(category_cnt_file)
