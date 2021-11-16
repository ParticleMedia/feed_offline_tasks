import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
from get_doc_feature import *

def getDocFeatureWithRetry(docid, retryCnt):
    for i in range(0, retryCnt):
        docInfo = getDocFeature(docid, 'text_category')
        if docInfo is not None:
            return docInfo
    return None

if __name__ == "__main__":
    curDocInfo = None
    curDocId = None
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 3:
            hadoop_counter.increase_counter('join', 'invalid_line')
            continue

        docid = splits[0]
        if curDocInfo is None or docid != curDocInfo['_id']:
            try:
                curDocInfo = getDocFeatureWithRetry(docid, 3)
                if curDocInfo is None:
                    if docid != curDocId:
                        hadoop_counter.increase_counter('join', 'get_doc_failed')
                        sys.stderr.write('get doc %s feature missed\n' % docid)
                elif docid == curDocId:
                    hadoop_counter.increase_counter('join', 'get_doc_retry_success')
                    sys.stderr.write('get doc %s feature retry success\n' % docid)
                else:
                    hadoop_counter.increase_counter('join', 'get_doc_success')
            except Exception as e:
                traceback.print_exc()
                if docid != curDocId:
                    sys.stderr.write('get doc %s feature failed\n' % docid)
                    hadoop_counter.increase_counter('join', 'get_doc_exception')
                curDocInfo = None
            curDocId = docid

        if curDocInfo is None:
            hadoop_counter.increase_counter('join', 'doc_get_category_failed')
            continue
        try:
            dataStr = splits[1]
            data = json.loads(dataStr)
            userid = data['user_id']
            if 'text_category' in curDocInfo:
                data['text_category'] = curDocInfo['text_category']
                dataStr = json.dumps(data)
                hadoop_counter.increase_counter('join', 'success')
            else:
                hadoop_counter.increase_counter('join', 'doc_no_category')
            print '%s\t%s\t%s' % (userid, dataStr, splits[2])
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load doc json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('join', 'invalid_json')
            continue
    hadoop_counter.print_counter()