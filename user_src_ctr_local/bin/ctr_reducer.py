import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils

hadoop_counter = utils.Counter()

class CategoryCtr:
    def __init__(self, key):
        self.key = key
        self.category_ctr = {}
        self.total_clk = 0

    def append(self, record):
        self.appendCategory(record.source, record.clicked)
        if record.clicked:
            self.total_clk += 1
    
    def appendCategory(self, cate, clicked):
        if cate not in self.category_ctr:
            self.category_ctr[cate] = {'click': 0, 'check': 0}
        cateCtr = self.category_ctr[cate]
        cateCtr['check'] += 1
        if clicked > 0:
            cateCtr['click'] += 1

    def print_output(self, checkThreshold, clickThreshold):
        global hadoop_counter
        filtered = {k: v for k, v in self.category_ctr.iteritems() if v['check'] >= checkThreshold and v['click'] >= clickThreshold}
        if len(filtered) > 0:
            print '%s\t%s' % (self.key, json.dumps(filtered))
            hadoop_counter.increase_counter('reduce', 'output')
        else:
            hadoop_counter.increase_counter('reduce', 'filter')
            sys.stderr.write('filter %s %s\n' % (self.key, self.category_ctr))


class Record:
    def __init__(self, splits):
        self.key = splits[0]
        self.docid = splits[1]
        self.ts = int(splits[2])
        self.clicked = int(splits[3])
        self.source = splits[4]
        
if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s check_threshold click_threshold\n" % sys.argv[0])
        sys.exit(1)
    
    checkThreshold = int(sys.argv[1])
    clickThreshold = int(sys.argv[2])
    sys.stderr.write('thresholds check: %d, click: %d\n' % (checkThreshold, clickThreshold))

    ctrInfo = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 5:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue
        
        # click category
        try:
            record = Record(splits)
            if ctrInfo is None or record.key != ctrInfo.key:
                if ctrInfo is not None:
                    ctrInfo.print_output(checkThreshold, clickThreshold)
                ctrInfo = CategoryCtr(record.key)
            ctrInfo.append(record)
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')
    
    if ctrInfo is not None:
        ctrInfo.print_output(checkThreshold, clickThreshold)
    hadoop_counter.print_counter()
