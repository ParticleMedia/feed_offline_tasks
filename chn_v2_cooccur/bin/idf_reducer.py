import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils

hadoop_counter = utils.Counter()

if __name__ == "__main__":
    cate = None
    count = 0.0
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        # click category
        try:
            if cate is None or splits[0] != cate:
                if cate is not None:
                    hadoop_counter.increase_counter('reduce', 'output')
                    print('\t'.join([cate, '%.6f' % count]))
                cate = splits[0]
                count = 0.0
            count += float(splits[1])
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            hadoop_counter.increase_counter('reduce', 'failed')

    if cate is not None:
        hadoop_counter.increase_counter('reduce', 'output')
        print('\t'.join([cate, '%.6f' % count]))
    hadoop_counter.print_counter()
