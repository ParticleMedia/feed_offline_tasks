import sys
sys.path.insert(0, '')
import json
import traceback
import time
import math
import utils

EMBEDDING_SEPERATOR = ' '
PIVOT_DAYS = 7

class UserScorer:
    def __init__(self, uid):
        self.uid = uid
        self.clks = []

    def push(self, clk):
        self.clks.append(clk)
    
    def print_score(self):
        if len(self.clks) > 0:
            clk = float(len(self.clks))
            score = 2 + math.log10(float(clk) + 1.0)
            print '%s\t%f' % (self.uid, score)

if __name__ == "__main__":
    pivot = time.time() - 3600 * 24 * PIVOT_DAYS
    sys.stderr.write('pivot: %d\n' % pivot)
    hadoop_counter = utils.Counter()
    scorer = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('scoring', 'invalid_line')
            continue

        try:
            uid = splits[0]
            data = json.loads(splits[1])

            ts = data.get('ts', 0)
            if ts < pivot:
                hadoop_counter.increase_counter('scoring', 'filter')
                continue

            if scorer is None:
                scorer = UserScorer(uid)
            elif scorer.uid != uid:
                scorer.print_score()
                scorer = UserScorer(uid)
        
            ts = data.get('ts', -1)
            scorer.push(data)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('scoring', 'invalid_json')

    if scorer is not None:
        scorer.print_score()
        hadoop_counter.increase_counter('scoring', 'success')
    hadoop_counter.print_counter()
