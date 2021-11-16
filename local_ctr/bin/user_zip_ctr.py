import sys
sys.path.insert(0, '')
import json
import traceback
import time
import math
import collections

import utils

hadoop_counter = utils.Counter()

class ZipInfo:
    def __init__(self, zipcode):
        self.zipcode = zipcode
        self.last_check = -1
        self.last_click = -1
        self.check = 0
        self.click = 0

    def add(self, ts, clicked):
        self.last_check = max(self.last_check, ts)
        self.check += 1
        if clicked:
            self.last_click = max(self.last_click, ts)
            self.click += 1

    def toDict(self):
        return {
            'last_check': self.last_check,
            'last_click': self.last_click,
            'check' : self.check,
            'click' : self.click,
        }

class UserInfo:
    def __init__(self, uid):
        self.id = uid
        self.zip_ctr = {}

    def addZipcode(self, zipcode, ts, clicked):
        if zipcode not in self.zip_ctr:
            self.zip_ctr[zipcode] = ZipInfo(zipcode)
        self.zip_ctr[zipcode].add(ts, clicked)

    def append(self, cjvData):
        zipcode = cjvData.get('key', '')
        if zipcode == 'control':
            zipcode = cjvData.get('zip', '')
        if len(zipcode) == 0:
            return

        zipcode = zipcode.split('@')[0].strip('"')

        clicked = (cjvData['clicked'] and cjvData.get('pv_time', 0) > 2000)
        ts = cjvData['ts']
        self.addZipcode(zipcode, ts, clicked)

    def output(self):
        global hadoop_counter
        data = {}
        for zipcode, ctr in self.zip_ctr.items():
            if ctr.check >= 10:
                data[zipcode] = ctr.toDict()
        if len(data) == 0:
            return
        try:
            outStr = json.dumps(data)
            print '%s\t%s' % (self.id, outStr)
            hadoop_counter.increase_counter('ctr', 'output')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('dump json failed: %s\n' % data)
            hadoop_counter.increase_counter('ctr', 'dump_fail')

if __name__ == "__main__":
    curUser = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('ctr', 'invalid_line')
            continue

        try:
            data = json.loads(splits[1])
            userid = splits[0]

            if curUser is None:
                curUser = UserInfo(userid)
            elif curUser.id != userid:
                curUser.output()
                curUser = UserInfo(userid)

            curUser.append(data)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('process line failed: %s\n' % line)
            hadoop_counter.increase_counter('ctr', 'failed')
    
    if curUser is not None:
        curUser.output()
    hadoop_counter.print_counter()
