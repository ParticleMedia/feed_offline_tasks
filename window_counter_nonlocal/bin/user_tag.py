import sys
sys.path.insert(0, '')
import math
import datetime
import time
import traceback
import json
import utils

CATEGORY_VIEW_THR=100
CTR_RATIO_POS_THR=1.5
CTR_RATIO_NEG_THR=0.67

def load_category_ctr(filepath):
    category_ctr = {}
    drop = 0
    with open(filepath) as fd:
        for line in fd:
            line = line.strip()
            if len(line) == 0:
                continue

            splits = line.split('\t')
            info = json.loads(splits[1])
            if info.get('v', 0) < CATEGORY_VIEW_THR:
                drop += 1
                continue

            info['ctr'] = float(info.get('c', 0)) / float(info.get('v', 0))
            category_ctr[splits[0]] = info
    sys.stderr.write('load category %d, drop %d\n' % (len(category_ctr), drop))
    return category_ctr


if __name__ == "__main__":
    category_ctr_file = sys.argv[1]
    field = sys.argv[2]
    level = int(sys.argv[3])
    debug = len(sys.argv) >= 5 and sys.argv[4] == 'true'

    neg_check_thr = 5
    pos_click_thr = 4
    if field == 'channels':
        neg_check_thr = 5
        pos_click_thr = 3
    sys.stderr.write('field:%s neg_check_thr: %d pos_click_thr: %d\n' % (field, neg_check_thr, pos_click_thr))
    category_ctr = load_category_ctr(category_ctr_file)
    for line in sys.stdin:
            line = line.strip()
            if len(line) == 0:
                continue

            splits = line.split('\t')
            uid = splits[0]
            info = json.loads(splits[1])

            pos_clk = []
            pos_ctr = []
            neg_clk = []
            neg_ctr = []
            for k, v in info.items():
                if level >= 0 and k.count('_') != level:
                    # ignore first_cat
                    continue
                if k not in category_ctr:
                    continue

                cat_ctr = category_ctr[k].get('ctr', 0.0)
                ctr = 0.0 if v.get('v', 0) == 0 else float(v.get('c', 0)) / float(v.get('v'))
                ctr_ratio = 1.0 if cat_ctr <= 0.0 else ctr / cat_ctr
                if v.get('c', 0) >= pos_click_thr:
                    pos_clk.append(k)
                    if ctr_ratio >= CTR_RATIO_POS_THR:
                        pos_ctr.append(k)
                if v.get('v', 0) >= neg_check_thr:
                    if ctr_ratio <= CTR_RATIO_NEG_THR:
                        neg_ctr.append(k)
                        if v.get('c', 0) == 0:
                            neg_clk.append(k)
            if debug:
                sys.stderr.write('uid %s pos_clk: %s pos_ctr: %s neg_ctr %s neg_clk %s\n' % (uid, pos_clk, pos_ctr, neg_ctr, neg_clk))
            print "%s\t%d\t%d\t%d\t%d" % (uid, len(pos_clk), len(pos_ctr), len(neg_ctr), len(neg_clk))

