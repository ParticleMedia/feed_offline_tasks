#input:  uid \t docid \t ts \t docInfo \t clicked ...
#output: uid \t docid \t ts \t category list \t checked \t clicked ...

import sys
sys.path.insert(0, '')
import traceback
import json
import utils
import action_sc


def normalize_sc_chn(sc_chns):
    return [round(sc_chn / sc_chns[0], 8) for sc_chn in sc_chns]

def main():
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 5:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue

        try:

            docInfo = json.loads(splits[3])
            if 'channels' not in docInfo or len(docInfo.get('channels', [])) == 0:
                hadoop_counter.increase_counter('map', 'no_category')
                continue

            if 'sc_channels' not in docInfo or len(docInfo.get('sc_channels', [])) == 0:
                hadoop_counter.increase_counter('map', 'no_category_sc')
                continue

            if len(docInfo.get('channels', [])) != len(docInfo.get('sc_channels', [])):
                hadoop_counter.increase_counter('map', 'sc_category_len_not_match')
                continue

            actionSplits = ['1']
            actionSplits.extend(splits[4:])
            act = action_sc.parseAction(actionSplits)
            act.normalize()
            if not act.isValidCheck():
                hadoop_counter.increase_counter('map', 'filter_by_cvtime')
                continue

            outSplits = [splits[0], splits[1], splits[2],
                         json.dumps(tuple(zip(docInfo['channels'],normalize_sc_chn(docInfo['sc_channels']))))]
            outSplits.extend(act.toList())
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

