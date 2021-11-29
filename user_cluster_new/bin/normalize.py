import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
import math

hadoop_counter = utils.Counter()

def parse(line):
    if len(line) == 0:
        return None
    
    splits = line.split('\t')
    if len(splits) != 4:
        sys.stderr.write('invalid line: %s\n' % line)
        return None
    em = splits[3].split(',')
    embedding = map(lambda x : float(x), em)
    return UserEmbeddingInfo(splits[0], int(splits[1]), float(splits[2]), embedding)

class UserEmbeddingInfo:
    def __init__(self, uid, latest_click, click_cnt, embedding):
        self.uid = uid
        self.embedding = embedding
        self.latest_click = latest_click
        self.clicks = click_cnt

    def normalize(self):
        squareSum = float(0.0)
        for i in range(0, len(self.embedding)):
            squareSum += (self.embedding[i] * self.embedding[i])
        norm2 = math.sqrt(squareSum)
        if norm2 <= 0.0:
            return
        
        for i in range(0, len(self.embedding)):
            self.embedding[i] /= norm2
    
    def printOutput(self):
        if self.clicks <= 0:
            return
        outDict = {
            'em': self.embedding,
            'click': self.clicks,
            'last_click_ts': self.latest_click,
        }
        print '%s\t%s' % (self.uid, json.dumps(outDict))

if __name__ == "__main__":
    for line in sys.stdin:
        record = parse(line.strip())
        if record is None:
            hadoop_counter.increase_counter('normalize', 'invalid_line')
            continue

        #record.normalize()
        record.printOutput()
        hadoop_counter.increase_counter('normalize', 'success')
    hadoop_counter.print_counter()
