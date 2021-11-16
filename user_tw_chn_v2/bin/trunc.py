#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, '')
import json
import traceback

def trunc(data, limit):
    if limit <= 0 or len(data) <= limit:
        return data

    bottom = limit / 3
    top = limit - bottom
    # FIXME trunc by ctr
    #sortedList = list(sorted(data.items(), key=lambda kv:(kv[1]['c'] / kv[1]['v'], kv[1]['v']), reverse=True))
    sortedList = list(sorted(data.items(), key=lambda kv: float(kv[1]['c'])-0.083*float(kv[1]['v']), reverse=True))
    #print sortedList
    out = {}
    for kv in sortedList[0:top]:
        out[kv[0]] = kv[1]
    s = max(top, len(sortedList) - bottom)
    for kv in sortedList[s:]:
        out[kv[0]] = kv[1]
    return out

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("Usage python %s limit\n" % sys.argv[0])
        sys.exit(1)

    limit = int(sys.argv[1])
    sys.stderr.write('trunc item limit: %d\n' % limit)

    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) <= 1:
            sys.stderr.write('invalid line: %s\n' % line)
            continue

        count = sys.maxsize
        try:
            if len(splits) >= 3:
                count = int(splits[2])

            if limit <= 0 or count <= limit:
                print '%s\t%s' % (splits[0], splits[1])
            else:
                data = json.loads(splits[1])
                data = trunc(data, limit)
                print '%s\t%s' % (splits[0], json.dumps(data))
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)

