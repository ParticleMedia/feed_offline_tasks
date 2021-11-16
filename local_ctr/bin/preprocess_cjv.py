import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()

def filter_cjv(item):
    if item is None:
        return False
    if not item.get('checked', False):
        return False
    elif len(item.get('user_id', '')) == 0 and len(item.get('docid', '')) == 0:
        return False
    elif 'ts' not in item:
        return False
    elif 'nr_view' not in item:
        return False

    ctype = item.get('ctype', '')
    if len(ctype) > 0 and ctype != 'news':
        return False

    nr_view = item.get('nr_view', {})
    srcName = nr_view.get('src_channel_name', '')
    if srcName != 'foryou' and srcName != 'local':
        return False

    condition = nr_view.get('condition', '')
    if not condition.startswith('local'):
        return False
    return True

def process_cjv(cjvData):
    global hadoop_counter
    if not filter_cjv(cjvData):
        hadoop_counter.increase_counter('cjv', 'filter')
        return

    doc_id = cjvData['docid']
    user_id = cjvData['user_id']
    nr_view = cjvData['nr_view']

    out = {
        'user_id': user_id,
        'docid': doc_id,
        'ts': cjvData['ts'],
        'clicked': cjvData['clicked'],
        'shared': cjvData.get('shared', False),
        'liked': cjvData.get('liked', False),
        'blocked': cjvData.get('blocked', False),
        'thumbed_up': cjvData.get('thumbed_up', False),
        'thumbed_down': cjvData.get('thumbed_down', False),
        'cv_time': cjvData.get('cv_time', False),
        'pv_time': cjvData.get('pv_time', False),
        'zip': nr_view.get('zip', ''),
        'key': nr_view.get('key', ''),
    }

    try:
        outStr = json.dumps(out)
        print '%s\t%s\t%d' % (doc_id, outStr, cjvData['ts'])
        hadoop_counter.increase_counter('cjv', 'success')
    except Exception as e:
        traceback.print_exc()
        sys.stderr.write('dump json failed: %s\n' % out)
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

