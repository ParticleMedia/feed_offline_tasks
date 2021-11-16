## Spark Application - execute with spark-submit

## Imports
import json
import sys
import math
import datetime
import time
import traceback 
from collections import Counter

def load_category_idf(total_user_cnt, category_user_cnt_file):
    if total_user_cnt <= 0:
        return None
    
    cate_idf = {}
    with open(category_user_cnt_file) as cate_user_cnt_fd:
        for line in cate_user_cnt_fd:
            line = line.strip()
            if len(line) == 0:
                continue
            splits = line.split('\t')
            if len(splits) != 2:
                sys.stderr.write('invalid catetory line: %s\n' % line)
                continue
            
            category = splits[0]
            cnt = float(splits[1])
            idf = math.log(float(total_user_cnt) / cnt, 10)
            cate_idf[category] = idf
            sys.stderr.write('category %s idf %f\n' % (category, idf))
    sys.stderr.write('load category idf: %s\n' % len(cate_idf))
    return cate_idf

def main(total_user_cnt, category_user_cnt_file):
    cate_idf_dict = load_category_idf(total_user_cnt, category_user_cnt_file)
    if cate_idf_dict is None or len(cate_idf_dict) == 0:
        sys.stderr.write('failed to load category idf from: %s\n' % category_user_cnt_file)
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

        cate_list = []
        for cate, info in cate_data.items():
            idf = cate_idf_dict.get(cate, 0.0)
            tf = info.get('tf', 0.0)
            cate_list.append({
                'category': cate,
                'weight': tf * idf,
                'click': info.get('click', 0),
            })
        cate_list.sort(key=lambda x : x['weight'], reverse=True)
        try:
            weight_json = json.dumps(cate_list)
            print "%s\t%s" % (key, weight_json)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('dump user weight to json failed: %s\n' % cate_list)
    sys.stderr.write("parse click finished\n")

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s total_user_cnt category_user_cnt_file" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    total_user_cnt = int(sys.argv[1])
    category_user_cnt_file = sys.argv[2]
    if total_user_cnt <= 0:
        sys.stderr.write("total_user_cnt: %d should be positive" % total_user_cnt)
        sys.exit(1)

    main(total_user_cnt, category_user_cnt_file)
