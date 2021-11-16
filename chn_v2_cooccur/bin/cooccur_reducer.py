import sys
sys.path.insert(0, '')
import json
import traceback
import time
from collections import Counter
import utils

hadoop_counter = utils.Counter()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage python %s cooccur_num \n" % sys.argv[0])
        sys.exit(1)
    cooccur_num = int(sys.argv[1])

    cate = None
    cooccur_dict = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        # click category
        try:
            if cate is None or splits[0] != cate:
                if cate is not None and cooccur_dict is not None and len(cooccur_dict) > 0:
                    if len(cooccur_dict) < cooccur_num:
                        result = sorted(cooccur_dict.items(), key=lambda x: x[1], reverse=True)
                    else:
                        result = sorted(cooccur_dict.items(), key=lambda x: x[1], reverse=True)[:cooccur_num]
                    result = [[x[0], '%.6f' % (x[1])] for i, x in enumerate(result)]
                    print('\t'.join([cate, json.dumps(result)]))
                    hadoop_counter.increase_counter('reduce', 'output')
                cate = splits[0]
                cooccur_dict = {}
            cooccur_dict[splits[1]] = cooccur_dict.get(splits[1],0.0) + float(splits[2])
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            hadoop_counter.increase_counter('reduce', 'failed')

    if cate is not None and cooccur_dict is not None and len(cooccur_dict) > 0:
        if len(cooccur_dict) < cooccur_num:
            result = sorted(cooccur_dict.items(), key=lambda x: x[1], reverse=True)
        else:
            result = sorted(cooccur_dict.items(), key=lambda x: x[1], reverse=True)[:cooccur_num]
        result = [[x[0], '%.6f' % (x[1])] for i, x in enumerate(result)]
        print('\t'.join([cate, json.dumps(result)]))
        hadoop_counter.increase_counter('reduce', 'output')
    hadoop_counter.print_counter()
