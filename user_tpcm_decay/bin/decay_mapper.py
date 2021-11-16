import sys

sys.path.insert(0, '')
import traceback
import json
import utils


def main(decay_coef):
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
            ctrInfo = json.loads(splits[1])
            if (len(ctrInfo) == 0):
                hadoop_counter.increase_counter('map', 'no_category')
                continue

            for chn,actions in ctrInfo.items():
                for action, value in actions.items():
                    ctrInfo[chn][action] = round(float(value) * decay_coef, 6)
            outSplits = [splits[0],json.dumps(ctrInfo),splits[2]]
            print ('\t'.join(outSplits))
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'parse_line_fail')
    hadoop_counter.print_counter()


if __name__ == "__main__":
    # Execute Main functionality
    decay_coef = pow(0.5, (1.0 / 30))
    main(decay_coef)
