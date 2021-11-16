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
    uid_set = set()
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
            if cur_category is not None and len(uid_set) > 0:
                print '%s\t%d' % (cur_category, len(uid_set))
                hadoop_counter.increase_counter('reduce', 'output')
            uid_set = set()
            cur_category = category
        
        uid = splits[1].strip()
        uid_set.add(uid)
        hadoop_counter.increase_counter('reduce', 'success')
    
    if cur_category is not None and len(uid_set) > 0:
        print '%s\t%d' % (cur_category, len(uid_set))
        hadoop_counter.increase_counter('reduce', 'output')
    hadoop_counter.print_counter()
