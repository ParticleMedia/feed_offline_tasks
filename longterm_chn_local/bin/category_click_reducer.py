import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils

if __name__ == "__main__":
    hadoop_counter = utils.Counter()
    cur_category = None
    cate_click = 0
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        category = splits[0].strip()
        if cur_category is None or category != cur_category:
            if cur_category is not None and cate_click > 0:
                print '%s\t%d' % (cur_category, cate_click)
                hadoop_counter.increase_counter('reduce', 'output')
            cate_click = 0
            cur_category = category
        
        clk = int(splits[1].strip())
        cate_click += clk
        hadoop_counter.increase_counter('reduce', 'success')
    
    if cur_category is not None and cate_click > 0:
        print '%s\t%d' % (cur_category, cate_click)
        hadoop_counter.increase_counter('reduce', 'output')
    hadoop_counter.print_counter()
