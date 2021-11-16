import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils

hadoop_counter = utils.Counter()

DIMENSION=50
DECAY_COREF = pow(0.5, (1.0 / 30))

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

class UserEmbeddingInfo:
    def __init__(self, uid):
        self.uid = uid
        self.embedding = [float(0.0)] * DIMENSION
        self.latest_click = 0
        self.clicks = float(0.0)
        self.docid_set = set()

    def push(self, item, tsNow):
        global hadoop_counter
        ts = item.get('ts', -1)
        self.latest_click = max(self.latest_click, ts)
        daysDiff = (tsNow - ts) // 86400
        decay = pow(DECAY_COREF, daysDiff)
        
        docid = item.get('docid', '')
        em = item.get('doc_em', [])
        if len(docid) == 0 or len(em) != DIMENSION or docid in self.docid_set:
            sys.stderr.write('dedup docid %s for user %s\n' % (docid, self.uid))
            hadoop_counter.increase_counter('merge', 'dedup')
            return
        
        self.docid_set.add(docid)
        self.clicks += decay
        for i in range(0, DIMENSION):
            self.embedding[i] += decay * em[i]
        hadoop_counter.increase_counter('merge', 'success')
        
    
    def printOutput(self):
        if self.clicks <= 0:
            return
        embeddingStr = ','.join(map(lambda x : '%f' % x, self.embedding))
        print '%s\t%d\t%f\t%s' % (self.uid, self.latest_click, self.clicks, embeddingStr)

def filter_click(data):
    ts = data.get('ts', 0)
    if ts > 1597215600 and ts < 1597312800:
        return True
    return False

if __name__ == "__main__":
    curUserInfo = None
    tsNow = getTimestampOfToday()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('merge', 'invalid_line')
            continue

        uid = splits[0]
        if curUserInfo is None:
            curUserInfo = UserEmbeddingInfo(uid)
        elif curUserInfo.uid != uid:
            curUserInfo.printOutput()
            hadoop_counter.increase_counter('merge', 'output')
            curUserInfo = UserEmbeddingInfo(uid)
        
        try:
            data = json.loads(splits[1])
            if filter_click(data):
                hadoop_counter.increase_counter('merge', 'filter')
                continue
            curUserInfo.push(data, tsNow)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load json failed: %s\n' % line)
            hadoop_counter.increase_counter('merge', 'invalid_json')

    if curUserInfo is not None:
        curUserInfo.printOutput()
        hadoop_counter.increase_counter('merge', 'output')
    hadoop_counter.print_counter()
