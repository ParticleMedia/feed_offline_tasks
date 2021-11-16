import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils

hadoop_counter = utils.Counter()

class JoinInfo:
    def __init__(self, key):
        self.key = key
        self.category_click = Counter()
        self.total_clk = 0

    def append_click(self, category, clk):
        self.category_click.update(Counter({category: clk}))
        self.total_clk += clk

    def print_output(self, clickThreshold):
        global hadoop_counter
        if self.key is None or len(self.key) == 0 or len(self.category_click) == 0 or self.total_clk == 0:
            return
        
        #if self.total_clk <= clickThreshold:
        #    hadoop_counter.increase_counter('reduce', 'filter_by_threshold')
        #    return

        filtered_cate_clk = {k : v for k, v in self.category_click.iteritems() if v >= clickThreshold}
        total_clk = sum(filtered_cate_clk.values())
        if len(filtered_cate_clk) == 0:
            hadoop_counter.increase_counter('reduce', 'filter_by_threshold')
            return
        
        cate_tf = {}
        for cate, clk in filtered_cate_clk.items():
            tf = float(clk) / float(total_clk)
            cate_tf[cate] = {
                "click": clk,
                "tf": tf,
            }
        try:
            catestr = json.dumps(cate_tf)
            print '%s\t%s\t%d' % (self.key, catestr, self.total_clk)
            hadoop_counter.increase_counter('reduce', 'output')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('dump category json failed: %s\n' % cate_tf)
            hadoop_counter.increase_counter('reduce', 'dump_fail')

if __name__ == "__main__":
    clickThreshold = 0
    if len(sys.argv) > 1:
        clickThreshold = int(sys.argv[1])
        sys.stderr.write('click threshold: %d\n' % clickThreshold)

    joinInfo = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        key = splits[0]
        if joinInfo is None or key != joinInfo.key:
            if joinInfo is not None:
                joinInfo.print_output(clickThreshold)
            joinInfo = JoinInfo(key)
        
        # click category
        try:
            cjvData = json.loads(splits[1])
            channels = cjvData.get('channels_v2', [])
            for chn in channels:
                joinInfo.append_click(chn, 1)
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')
    
    if joinInfo is not None:
        joinInfo.print_output(clickThreshold)
    hadoop_counter.print_counter()
