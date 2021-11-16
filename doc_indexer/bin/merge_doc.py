import sys
sys.path.insert(0, '')
import json
import traceback
import time
import heapq
import utils

hadoop_counter = utils.Counter()
PIVOT_DAYS = 3

class DocInfo:
    def __init__(self, docid):
        self.docid = docid
        self.ts = -1
        self.embedding = None

    def update(self, ts, embedding):
        if ts >= self.ts:
            self.ts = ts
            self.embedding = embedding
    
    def print_embedding(self):
        if self.embedding is not None:
            em = map(lambda x : '%f' % x, self.embedding)
            print '%s\t%s' % (self.docid, ' '.join(em))

if __name__ == "__main__":
    pivot = time.time() - 3600 * 24 * PIVOT_DAYS

    curDoc = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('select', 'invalid_line')
            continue

        docid = splits[0]
        if curDoc is None:
            curDoc = DocInfo(docid)
        elif curDoc.docid != docid:
            curDoc.print_embedding()
            hadoop_counter.increase_counter('select', 'output')
            curDoc = DocInfo(docid)
        
        try:
            data = json.loads(splits[1])
            if 'doc_em' not in data:
                hadoop_counter.increase_counter('select', 'no_embedding')
                continue
            elif data.get('epoch', -1) < pivot:
                hadoop_counter.increase_counter('select', 'staleness')
                continue
            curDoc.update(data['ts'], data['doc_em'])
            hadoop_counter.increase_counter('select', 'update')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('select', 'invalid_json')

    if curDoc is not None:
        curDoc.print_embedding()
        hadoop_counter.increase_counter('select', 'output')
    hadoop_counter.print_counter()
