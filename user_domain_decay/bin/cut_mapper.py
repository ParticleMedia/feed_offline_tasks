import sys

sys.path.insert(0, '')
import traceback
import json
import utils
from collections import OrderedDict

def main():
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('ctr', 'invalid_line')
            continue

        try:
            
            size = int(splits[2])
            if (size == 0):
                hadoop_counter.increase_counter('map', 'no_category')
                continue
            if (size < 300):
                print ('\t'.join(splits[:2]))
                hadoop_counter.increase_counter('map', 'cut_less_300')
            else:
                ctrInfo = json.loads(splits[1])
                sorted_items = sorted(ctrInfo.items(), key=lambda x: float(x[1]['c'])-0.083*float(x[1]['v']), reverse=True)
                sorted_dict = OrderedDict(sorted_items[:200] + sorted_items[-100:])
                hadoop_counter.increase_counter('map', 'cut_greater_300')
                outSplits = [splits[0],json.dumps(sorted_dict)]
                print ('\t'.join(outSplits))
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'parse_line_fail')
    hadoop_counter.print_counter()


if __name__ == "__main__":
    # Execute Main functionality
    main()
