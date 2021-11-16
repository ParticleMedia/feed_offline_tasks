import sys
sys.path.insert(0, '')
import json
import traceback

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        try:
            splits = line.split('\t')
            data = json.loads(splits[1])
            data = filter(lambda x : not x['category'].startswith('Crime'), data)
            if len(data) > 0:
                print '%s\t%s' % (splits[0], json.dumps(data))
        except Exception as e:
            traceback.print_exc()