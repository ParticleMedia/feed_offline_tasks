import sys
import json

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.startswith('grp#chnv2#') or line.find('#android#3d') < 0:
            continue

        parts = line.split('\t')
        chn = parts[0].split('#')[2]
        ctr = json.loads(parts[1])
        view = int(ctr['overall'].split(' ')[0])
        click = int(ctr['overall'].split(' ')[1])
        print '%s,%d,%d' % (chn, view, click)
