import sys
sys.path.insert(0, '')
import json
import traceback
import time
import heapq
import utils

hadoop_counter = utils.Counter()

class UserClickQueue:
    def __init__(self, uid, queue_len):
        self.uid = uid
        self.queue_len = queue_len
        self.queue = []
        self.docid_set = set()

    def push(self, item):
        global hadoop_counter
        ts = item.get('ts', -1)
        docid = item.get('docid', '')
        if len(docid) == 0 or docid in self.docid_set:
            sys.stderr.write('dedup docid %s for user %s\n' % (docid, self.uid))
            hadoop_counter.increase_counter('select', 'dedup')
            return
        hadoop_counter.increase_counter('select', 'enqueue')
        heapq.heappush(self.queue, (ts, item))
        self.docid_set.add(docid)
        if len(self.queue) > self.queue_len:
            heapq.heappop(self.queue)
            hadoop_counter.increase_counter('select', 'discard')
    
    def print_queue(self):
        global hadoop_counter
        for item in self.queue:
            try:
                outStr = json.dumps(item[1])
                print '%s\t%s' % (self.uid, outStr)
                hadoop_counter.increase_counter('select', 'select')
            except Exception as e:
                traceback.print_exc()
                sys.stderr.write('dump json failed: %s\n' % item)
                hadoop_counter.increase_counter('select', 'dump_failed')
                continue

def filter_click(data):
    text_cat_score = data.get('text_cat_score', {})
    if 'crime' in text_cat_score or 'publicsafety' in text_cat_score or 'sensitive' in text_cat_score:
        return True
    return False

if __name__ == "__main__":
    queue_len = 10
    if len(sys.argv) > 1:
        queue_len = int(sys.argv[1])
    
    sys.stderr.write('click queue len: %d\n' % queue_len)
    curUserQueue = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('select', 'invalid_line')
            continue

        uid = splits[0]
        if curUserQueue is None:
            curUserQueue = UserClickQueue(uid, queue_len)
        elif curUserQueue.uid != uid:
            curUserQueue.print_queue()
            curUserQueue = UserClickQueue(uid, queue_len)
        
        try:
            data = json.loads(splits[1])
            if filter_click(data):
                hadoop_counter.increase_counter('select', 'filter')
                continue
            curUserQueue.push(data)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('select', 'invalid_json')

    if curUserQueue is not None:
        curUserQueue.print_queue()
    hadoop_counter.print_counter()
