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

def parse(line):
    if len(line) == 0:
        return None
    
    splits = line.split('\t')
    if len(splits) != 4:
        sys.stderr.write('invalid line: %s\n' % line)
        return None
    em = splits[3].split(',')
    if len(em) != DIMENSION:
        sys.stderr.write('invalid em: %s\n' % line)
        return None

    embedding = map(lambda x : float(x), em)
    return UserEmbeddingInfo(splits[0], int(splits[1]), float(splits[2]), embedding)

class UserEmbeddingInfo:
    def __init__(self, uid, latest_click, click_cnt, embedding):
        self.uid = uid
        self.embedding = embedding
        self.latest_click = latest_click
        self.clicks = click_cnt

    def merge(self, other):
        self.latest_click = max(self.latest_click, other.latest_click)
        self.clicks += other.clicks
        for i in range(0, DIMENSION):
            self.embedding[i] += other.embedding[i]
        hadoop_counter.increase_counter('merge', 'success')

    def decay(self, decayParam):
        self.clicks *= decayParam
        for i in range(0, len(self.embedding)):
            self.embedding[i] *= decayParam
    
    def printOutput(self):
        if self.clicks <= 0:
            return
        self.decay(DECAY_COREF)
        embeddingStr = ','.join(map(lambda x : '%f' % x, self.embedding))
        print '%s\t%d\t%f\t%s' % (self.uid, self.latest_click, self.clicks, embeddingStr)

if __name__ == "__main__":
    curUserInfo = None
    for line in sys.stdin:
        record = parse(line.strip())
        if record is None:
            hadoop_counter.increase_counter('merge', 'invalid_line')
            continue

        uid = record.uid
        if curUserInfo is None:
            curUserInfo = record
        elif curUserInfo.uid != uid:
            curUserInfo.printOutput()
            hadoop_counter.increase_counter('merge', 'output')
            curUserInfo = record
        else:
            curUserInfo.merge(record)
        hadoop_counter.increase_counter('merge', 'success')
    if curUserInfo is not None:
        curUserInfo.printOutput()
        hadoop_counter.increase_counter('merge', 'output')
    hadoop_counter.print_counter()
