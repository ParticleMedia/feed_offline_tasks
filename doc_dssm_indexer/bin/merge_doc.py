import sys
sys.path.insert(0, '')
import json
import traceback
import time
import heapq

class DocInfo:
    def __init__(self, docid):
        self.docid = docid
        self.ts = -1
        self.data = {}

    def update(self, ts, data):
        if ts >= self.ts:
            self.ts = ts
            self.data = data

    def print_data(self):
        if self.data:
            print '%s\t%s' % (self.docid, json.dumps(self.data))

if __name__ == "__main__":
    curDoc = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            continue

        docid = splits[0]
        if curDoc is None:
            curDoc = DocInfo(docid)
        elif curDoc.docid != docid:
            curDoc.print_data()
            curDoc = DocInfo(docid)

        data = json.loads(splits[1])
        curDoc.update(data['ts'], data)

    if curDoc is not None:
        curDoc.print_data()

