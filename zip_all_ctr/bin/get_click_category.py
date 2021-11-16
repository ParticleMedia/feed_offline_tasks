#input:  docid \t uid \t ts \t clicked
#output: uid \t docid \t ts \t clicked \t text_category

import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
from get_doc_feature import *

STAT_FIELDS = ['text_category','tpc_s', 'source']
#STAT_FIELDS = ['text_category_v2']

def getDocFeatureWithRetry(docid, retryCnt):
    for i in range(0, retryCnt):
        docInfo = getDocFeature(docid, ','.join(STAT_FIELDS))
        if docInfo is not None:
            # pre process each field to list
            if 'text_category' in docInfo:
                docInfo['text_category'] = flatten_category(docInfo['text_category'])
            if 'tpc_s' in docInfo:
                docInfo['tpc_s'] = filter_tpcs(docInfo['tpc_s'], 0.3)
            if 'source' in docInfo and len(docInfo['source']) > 0:
                docInfo['source'] = [docInfo['source']]
            return docInfo
    return None

def flatten_category(cateObj):
    if cateObj is None:
        return None
    return cateObj.get('first_cat', {}).keys() + cateObj.get('second_cat', {}).keys() + cateObj.get('third_cat', {}).keys()

def filter_tpcs(tpcs, thr):
    if tpcs is None:
        return None
    return [key for (key, value) in tpcs.items() if value >= thr]

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

        if self.docDict is not None:
            for field in STAT_FIELDS:
                if field not in self.docDict:
                    sys.stderr.write('doc %s has no category %s \n' % (docid,field))
                    hadoop_counter.increase_counter('join', 'missing_category %s' % (field))
                else:
                    hadoop_counter.increase_counter('join', 'hit_category %s ' % (field))

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
def toZip(s):
  try:
    result = int(s)
    if (result > 99999):
        sys.stderr.write('invalid_zip %s\n' % s)
        hadoop_counter.increase_counter('join', 'invalid zip')
        return None
    return s
  except Exception as e:
    sys.stderr.write('invalid_zip %s\n' % s)
    hadoop_counter.increase_counter('join', 'invalid zip')
    return None

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
        key = toZip(splits[1].strip('"').split('@')[0])

        if key == None:
            hadoop_counter.increase_counter('cjv', 'filter')
            continue
        # fetch doc info firstly
        if curDocInfo is None or docid != curDocInfo.docid:
            curDocInfo = DocInfo(docid)
        curDocInfo.fetchDocInfo()

        if curDocInfo.docDict is None:
            hadoop_counter.increase_counter('join', 'doc_get_category_failed')
            continue
        try:
            # if curDocInfo.isLocal():
            #     hadoop_counter.increase_counter('join', 'local_doc')
            #     continue

            curDocJson = curDocInfo.toJson()
            if curDocJson is None:
                hadoop_counter.increase_counter('join', 'no_category')
            else:
                # swap uid and docid
                # insert docInfo after ts
                # uid = splits[1]
                splits[0] = key
                splits[1] = docid
                splits.append(curDocJson)
                hadoop_counter.increase_counter('join', 'success')
                print '\t'.join(splits)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load doc json failed: %s\n' % line)
            hadoop_counter.increase_counter('join', 'invalid_json')
            continue
    hadoop_counter.print_counter()



# def getDocFeatureWithRetry(docid, retryCnt):
#     for i in range(0, retryCnt):
#         docInfo = getDocFeature(docid, 'text_category','')
#         if docInfo is not None:
#             if 'text_category' in docInfo:
#                 docInfo['text_category'] = json.dumps(docInfo['text_category'])
#             return docInfo
#     return None

# if __name__ == "__main__":
#     curDocInfo = None
#     curDocId = None
#     hadoop_counter = utils.Counter()
#     for line in sys.stdin:
#         line = line.strip()
#         if len(line) == 0:
#             continue

#         splits = line.split('\t')
#         docid = splits[0]
#         if curDocInfo is None or docid != curDocInfo['_id']:
#             try:
#                 curDocInfo = getDocFeatureWithRetry(docid, 3)
#                 if curDocInfo is None:
#                     if docid != curDocId:
#                         hadoop_counter.increase_counter('join', 'get_doc_failed')
#                         sys.stderr.write('get doc %s feature missed\n' % docid)
#                 elif docid == curDocId:
#                     hadoop_counter.increase_counter('join', 'get_doc_retry_success')
#                     sys.stderr.write('get doc %s feature retry success\n' % docid)
#                 else:
#                     hadoop_counter.increase_counter('join', 'get_doc_success')
#             except Exception as e:
#                 traceback.print_exc()
#                 if docid != curDocId:
#                     sys.stderr.write('get doc %s feature failed\n' % docid)
#                     hadoop_counter.increase_counter('join', 'get_doc_exception')
#                 curDocInfo = None
#             curDocId = docid
            
#             if curDocInfo is not None:
#                 if 'text_category' not in curDocInfo:
#                     sys.stderr.write('doc %s has no category\n' % docid)
#                     hadoop_counter.increase_counter('join', 'missing_category')
#                 else:
#                     hadoop_counter.increase_counter('join', 'hit_category')

#         if curDocInfo is None:
#             hadoop_counter.increase_counter('join', 'doc_get_category_failed')
#             continue
#         try:
#             if 'text_category' in curDocInfo:
#                 # swap splits[0] and splits[1]
#                 uid = splits[1]
#                 splits[0] = uid
#                 splits[1] = docid
#                 splits.append(curDocInfo['text_category'])
#                 hadoop_counter.increase_counter('join', 'success')
#                 print '\t'.join(splits)
#             else:
#                 hadoop_counter.increase_counter('join', 'doc_no_category')
#         except Exception as e:
#             traceback.print_exc()
#             sys.stderr.write('load doc json failed: %s\n' % splits[1])
#             hadoop_counter.increase_counter('join', 'invalid_json')
#             continue
#     hadoop_counter.print_counter()
