import sys
sys.path.insert(0, '')
import math
import datetime
import time
import traceback
import json
import utils
import action_sc

hadoop_counter = utils.Counter()

decayCoref = pow(0.5, (1.0 / 30))

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

class CategoryCtr:
    def __init__(self, key):
        self.key = key
        self.category_ctr = {}
        self.total_clk = 0

    def append_sc_chn(self, record):
        if len(record.categories) == 0:
            return
        for cat in record.categories:
            self.appendCategory_sc_chn(cat, record.action, record.decay)

    def appendCategory_sc_chn(self, cate, act, decay):
        if cate[0] not in self.category_ctr:
            self.category_ctr[cate[0]] = action_sc.ActionAgg()
        cateCtr = self.category_ctr[cate[0]]
        cateCtr.accumulate_ts_decay(cate[1], act, decay)

    def print_output(self, checkThreshold, clickThreshold):
        global hadoop_counter
        filtered = {k: v.toDict() for k, v in self.category_ctr.iteritems() if v.check >= checkThreshold and v.click >= clickThreshold}
        if len(filtered) > 0:
            print ('%s\t%s\t%d' % (self.key, json.dumps(filtered), len(filtered)))
            hadoop_counter.increase_counter('reduce', 'output')
            hadoop_counter.increase_counter('reduce', 'poi_cnt', len(filtered))
        else:
            hadoop_counter.increase_counter('reduce', 'filter')
            #sys.stderr.write('filter %s %s\n' % (self.key, self.category_ctr))

class Record:
    def __init__(self, splits, tsNow):
        self.key = splits[0]
        self.docid = splits[1]
        self.ts = int(splits[2])
        self.categories = json.loads(splits[3])
        self.action = action_sc.parseAction(splits[4:])

        daysDiff = 1 + ((tsNow - self.ts) / 86400)
        self.decay = pow(decayCoref, daysDiff)

if __name__ == "__main__":
    ctrInfo = None
    tsNow = getTimestampOfToday()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) <= 11:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        # click category
        try:
            record = Record(splits, tsNow)
            if ctrInfo is None or record.key != ctrInfo.key:
                if ctrInfo is not None:
                    ctrInfo.print_output(1e-6, 0)
                ctrInfo = CategoryCtr(record.key)
            ctrInfo.append_sc_chn(record)
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')

    if ctrInfo is not None:
        ctrInfo.print_output(1e-6, 0)
    hadoop_counter.print_counter()

