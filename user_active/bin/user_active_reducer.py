import sys
sys.path.insert(0, '')
import traceback
import json

import utils

hadoop_counter = utils.Counter()

class ChannelActiveData:
    def __init__(self):
        self.check = 0
        self.click = 0
    
    def append(self, clicked):
        self.check += 1
        if clicked > 0:
            self.click += 1
    
    def toDict(self):
        return {
            'checks': self.check,
            'clicks': self.click,
        }

class UserActiveData:
    def __init__(self, uid):
        self.uid = uid
        self.data = {}

    def append(self, channel, clicked):
        if channel not in self.data:
            self.data[channel] = ChannelActiveData()
        self.data[channel].append(clicked)

    def printOutput(self):
        global hadoop_counter
        try:
            outData = {k : v.toDict() for k, v in self.data.items()}
            print '%s\t%s' % (self.uid, json.dumps(outData))
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('dump json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')

if __name__ == "__main__":
    userData = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        try:
            uid = splits[0]
            if userData is None or uid != userData.uid:
                if userData is not None:
                    userData.printOutput()
                userData = UserActiveData(uid)
        
            channel = splits[1]
            clicked = int(splits[2])
            userData.append(channel, clicked)
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
    
    if userData is not None:
        userData.printOutput()
    hadoop_counter.print_counter()