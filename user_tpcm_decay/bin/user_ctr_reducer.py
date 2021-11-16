import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
import action

hadoop_counter = utils.Counter()

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)


class CategoryCtr:
    def __init__(self, key):
        self.key = key
        self.category_ctr = {}
        self.total_clk = 0

    def append_sc(self, record,unitDecayCoef, score_field):
        if len(record.categories) == 0:
            return
        self.total_clk += record.action.clicked
        for cat in record.categories.items():
            self.appendCategory_sc(cat, record.action, record.ts, unitDecayCoef)

    def appendCategory_sc(self, cate, act, ts, unitDecayCoef):
        if cate[0] not in self.category_ctr:
            self.category_ctr[cate[0]] = action.ActionAgg(unitDecayCoef)
        cateCtr = self.category_ctr[cate[0]]
        #cateCtr.accumulate(act, ts)
        cateCtr.accumulate_ts_decay(cate[1],act, ts, getTimestampOfToday())

    def print_output(self, checkThreshold, clickThreshold):
        global hadoop_counter
        filtered = {k: v.toDict() for k, v in self.category_ctr.iteritems() if v.check >= checkThreshold and v.click >= clickThreshold}
        if len(filtered) > 0:
            print ('%s\t%s\t%d' % (self.key, json.dumps(filtered), len(filtered)))
            hadoop_counter.increase_counter('reduce', 'output')
        else:
            hadoop_counter.increase_counter('reduce', 'filter')
            #sys.stderr.write('filter %s %s\n' % (self.key, self.category_ctr))

class Record:
    def __init__(self, splits):
        self.key = splits[0]
        self.docid = splits[1]
        self.ts = int(splits[2])
        self.categories = json.loads(splits[3])
        self.action = action.parseAction(splits[4:],[])
        
if __name__ == "__main__":
    if len(sys.argv) < 5:
        sys.stderr.write("Usage python %s check_threshold click_threshold unitDecayCoef score_field\n" % sys.argv[0])
        sys.exit(1)
    
    checkThreshold = int(sys.argv[1])
    clickThreshold = int(sys.argv[2])
    if (sys.argv[3] == "TRUE"):
        unitDecayCoef = True
    else:
        unitDecayCoef = False
    score_field = sys.argv[4]
    sys.stderr.write('thresholds check: %d, click: %d, unitDecayCoef: %s, score_field %s\n' % (checkThreshold, clickThreshold, unitDecayCoef, score_field))

    ctrInfo = None
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
            record = Record(splits)
            if ctrInfo is None or record.key != ctrInfo.key:
                if ctrInfo is not None:
                    ctrInfo.print_output(checkThreshold, clickThreshold)
                ctrInfo = CategoryCtr(record.key)
            ctrInfo.append_sc(record,unitDecayCoef, score_field)
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')
    
    if ctrInfo is not None:
        ctrInfo.print_output(checkThreshold, clickThreshold)
    hadoop_counter.print_counter()
