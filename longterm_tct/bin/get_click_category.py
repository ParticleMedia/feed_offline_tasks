import sys
sys.path.insert(0, '')
import json
import traceback
import time
import utils
from get_doc_feature import *

def getDocFeatureWithRetry(docid, retryCnt):
    for i in range(0, retryCnt):
        docInfo = getDocFeature(docid, 'text_category,is_local_news')
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
            if curDocInfo.get('is_local_news', '') == 'true' and 'Sports' not in curDocInfo.get('text_category', {}).get('first_cat', {}):
                hadoop_counter.increase_counter('join', 'local_doc')
            elif 'text_category' in curDocInfo and len(curDocInfo['text_category']) != 0:
                data = json.loads(splits[1])
                data['text_category'] = curDocInfo['text_category']
                outStr = json.dumps(data)
                hadoop_counter.increase_counter('join', 'success')
                print outStr
            else:
                hadoop_counter.increase_counter('join', 'doc_no_category')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load doc json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('join', 'invalid_json')
            continue
    hadoop_counter.print_counter()
