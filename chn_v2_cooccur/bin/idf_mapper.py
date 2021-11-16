#input:  uid \t cjv
#output: docid \t uid \t ts \t clicked

import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()

def process_line(user_id, data):
    global hadoop_counter
    for k,v in data.items():
        print '%s\t%.6f' % (k, float(v['c']))
    hadoop_counter.increase_counter('cjv', 'success')

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue
        try:
            data = json.loads(splits[1])
            process_line(splits[0], data)

        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load cjv json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('cjv', 'invalid_json')
    hadoop_counter.print_counter()
