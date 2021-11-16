import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils
import action

hadoop_counter = utils.Counter()

class CategoryCtr:
    def __init__(self, key):
        self.key = key
        self.ts = -1
        self.category_ctr = action.Action(['0'] * 8)

    def append(self, record):
        self.ts = max(self.ts, record.ts)
        self.category_ctr.combine(record.action)

    def print_output(self):
        outList = [self.key, '%d' % self.ts]
        outList.extend(self.category_ctr.toList())
        print '\t'.join(outList)


class Record:
    def __init__(self, splits):
        self.key = splits[0]
        self.ts = int(splits[1])
        self.action = action.parseAction(splits[2:])

if __name__ == "__main__":
    ctrInfo = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) <= 9:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('combine', 'invalid_line')
            continue

        # click category
        try:
            record = Record(splits)
            if ctrInfo is None or record.key != ctrInfo.key:
                if ctrInfo is not None:
                    hadoop_counter.increase_counter('combine', 'output')
                    ctrInfo.print_output()
                ctrInfo = CategoryCtr(record.key)
            ctrInfo.append(record)
            hadoop_counter.increase_counter('combine', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('combine', 'invalid_json')
    
    if ctrInfo is not None:
        hadoop_counter.increase_counter('combine', 'output')
        ctrInfo.print_output()
    hadoop_counter.print_counter()
