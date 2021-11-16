#input:  docid \t uid \t ts \t clicked...
#output: uid \t docid \t ts \t docInfo \t clicked...

import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
from get_doc_feature import *

STAT_FIELDS = ['sc_channels','channels','text_category']

def getDocFeatureWithRetry(docid, retryCnt):
    for i in range(0, retryCnt):
        docInfo = getDocFeature(docid, 'local_score,%s' % ','.join(STAT_FIELDS))
        if docInfo is not None:
            if 'text_category' in docInfo:
                docInfo['text_category'] = flatten_category(docInfo['text_category'])
            # pre process each field to list
            return docInfo
    return None

def flatten_category(cateObj):
    if cateObj is None:
        return None
    return cateObj.get('first_cat', {}).keys() + cateObj.get('second_cat', {}).keys() + cateObj.get('third_cat', {}).keys()

def isSport(docInfo):
    if 'text_category' not in docInfo:
        return False
    for cat in docInfo['text_category']:
        if cat.startswith('Sports'):
            return True
    return False

class DocInfo:
    def __init__(self, docid):
        self.docid = docid
        self.docDict = None
        self.docJson = None
        self.tryCnt = 0

    def fetchDocInfo(self):
        if self.docDict is not None or self.tryCnt >= 3:
            return
        try:
            self.docDict = getDocFeatureWithRetry(self.docid, 3)
            if self.docDict is None:
                if self.tryCnt == 0:
                    hadoop_counter.increase_counter('join', 'get_doc_failed')
                    sys.stderr.write('get doc %s feature missed\n' % self.docid)
            elif self.tryCnt > 0:
                hadoop_counter.increase_counter('join', 'get_doc_retry_success')
                sys.stderr.write('get doc %s feature retry success\n' % self.docid)
            else:
                hadoop_counter.increase_counter('join', 'get_doc_success')
        except Exception as e:
            traceback.print_exc()
            if self.tryCnt == 0:
                sys.stderr.write('get doc %s feature failed\n' % self.docid)
                hadoop_counter.increase_counter('join', 'get_doc_exception')
            self.docDict = None
        self.tryCnt += 1

    def toJson(self):
        if self.docJson is not None:
            return self.docJson
        elif self.docDict is not None:
            out = {}
            for f in STAT_FIELDS:
                items = self.docDict.get(f, [])
                if items is not None and len(items) > 0:
                    out[f] = items
                self.docJson = None if len(out) == 0 else json.dumps(out)
        else:
            self.docJson = None
        return self.docJson

    def isLocal(self):
        if self.docDict is None:
            return False
        else:
            return self.docDict.get('local_score', 0.0) > 0.5

if __name__ == "__main__":
    curDocInfo = None
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) <= 3:
            continue

        docid = splits[0]
        # fetch doc info firstly
        if curDocInfo is None or docid != curDocInfo.docid:
            curDocInfo = DocInfo(docid)
        curDocInfo.fetchDocInfo()

        if curDocInfo.docDict is None:
            hadoop_counter.increase_counter('join', 'doc_get_category_failed')
            continue
        try:
            if curDocInfo.isLocal():
                hadoop_counter.increase_counter('join', 'local_doc')
                continue

            curDocJson = curDocInfo.toJson()
            if curDocJson is None:
                hadoop_counter.increase_counter('join', 'no_category')
            else:
                # swap uid and docid
                # insert docInfo after ts
                outSplits = [splits[1], docid, splits[2], curDocJson]
                outSplits.extend(splits[3:])
                hadoop_counter.increase_counter('join', 'success')
                print '\t'.join(outSplits)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load doc json failed: %s\n' % line)
            hadoop_counter.increase_counter('join', 'invalid_json')
            continue
    hadoop_counter.print_counter()
