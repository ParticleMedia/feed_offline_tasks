import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()

def filter_cjv(item):
    if item is None:
        return False
    if not item.get('clicked', False) and not item.get('shared', False) and item.get('thumbed_up', False):
        return False
    elif item.get('thumbed_down', False):
        return False
    elif len(item.get('user_id', '')) == 0 and len(item.get('docid', '')) == 0:
        return False
    
    ctype = item.get('ctype', '')
    if ctype != 'news':
        return False

    condition = item.get('nr_view', {}).get('condition', '')
    if 'push_view' in cjvData:
        condition = item['push_view'].get('source', '')
    if not condition.startswith('local'):
        return False
    return True

def process_cjv(cjvData):
    global hadoop_counter
    if not filter_cjv(cjvData):
        hadoop_counter.increase_counter('cjv', 'filter')
        return
    
    docid = cjvData['docid']
    outDict = {
        'uid': cjvData['user_id'],
        'docid': cjvData['docid'],
        'key': cjvData.get('nr_view', {}).get('key', ''),
        'ts': cjvData['ts'],
        'log_src': cjvData.get('log_src', ''),
        'clicked': cjvData.get('clicked', False),
        'shared': cjvData.get('shared', False),
        'thumbed_up': cjvData.get('thumbed_up', False),
        'pv_time': cjvData.get('pv_time', 0)
    }
    
    try:
        outStr = json.dumps(outDict)
        print '%s\t%s' % (docid, outStr)
        hadoop_counter.increase_counter('cjv', 'success')
    except Exception as e:
        traceback.print_exc()
        sys.stderr.write('dump cjv json failed: %s\n' % outDict)
        hadoop_counter.increase_counter('cjv', 'dump_failed')

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if line.find('"checked":true,') < 0:
            continue

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue

        try:
            cjvData = json.loads(splits[1])
            process_cjv(cjvData)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load cjv json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('cjv', 'invalid_json')
    hadoop_counter.print_counter()
