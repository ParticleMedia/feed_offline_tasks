import sys
import json
import traceback
import time

sys.path.insert(0, '')
import utils

hadoop_counter = utils.Counter()
PIVOT_DAYS = 3

def process(docData, pivot):
    if '_id' not in docData or 'doc_em' not in docData:
        return None
    
    # disable index
    if docData.get('disable_index', False):
        return None


    # staleness
    epoch = docData.get('epoch', -1)
    if epoch < pivot:
        return None

    if 'Obituary' in docData.get('text_category', {}).get('first_cat', {}):
        return None

    # local news
    isLocalNews = docData.get('local_score', 0.0) > 0.5
    if isLocalNews:
        return None

    filtededDoc = {
        'docid': docData['_id'],
        'ts': docData.get('ts_task', -1),
        'epoch': epoch,
        'doc_em': docData['doc_em'],
    }
    return filtededDoc

if __name__ == "__main__":
    pivot = time.time() - 3600 * 24 * PIVOT_DAYS
    for line in sys.stdin:
        line = line.strip()

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('doc', 'invalid_line')
            continue

        try:
            docData = json.loads(splits[1])
            out = process(docData, pivot)
            if out is None:
                hadoop_counter.increase_counter('doc', 'filtered')
            else:
                docid = out['docid']
                print '%s\t%s' % (docid, json.dumps(out))
                hadoop_counter.increase_counter('doc', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load doc json failed: %s\n' % line)
            hadoop_counter.increase_counter('doc', 'invalid_json')
    hadoop_counter.print_counter()
        

