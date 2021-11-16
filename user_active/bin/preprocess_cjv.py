import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()

def isProcessPushCJV():
    env = utils.HadoopEnv()
    if env.is_running_on_hadoop():
        input_path = env.map_input_file
        sys.stderr.write('running on hadoop, input:%s\n' % input_path)
        return (input_path.find('push_cjv_snappy') >= 0)
    else:
        return False

def filter_cjv(item, isPush):
    if item is None:
        return False
    if not item.get('checked', False) and not item.get('push_recved', False) and not item.get('clicked', False):
        return False
    elif len(item.get('user_id', '')) == 0 and len(item.get('docid', '')) == 0:
        return False

    if not isPush:
        if 'nr_view' not in item:
            return False
        channel = item['nr_view'].get('src_channel_name', '')
        if channel != 'foryou' and channel != 'local':
            return False
    return True

def process_cjv(cjvData, isPush):
    global hadoop_counter
    if not filter_cjv(cjvData, isPush):
        hadoop_counter.increase_counter('cjv', 'filter')
        return
    
    channel = None
    if isPush:
        channel = 'push'
    elif 'nr_view' in cjvData:
        channel = cjvData['nr_view']['src_channel_name']
    else:
        # should not happen
        hadoop_counter.increase_counter('cjv', 'filter')
        return

    uid = cjvData['user_id']
    clicked = 1 if cjvData.get('clicked', False) else 0
    
    try:
        print '%s\t%s\t%d' % (uid, channel, clicked)
        hadoop_counter.increase_counter('cjv', 'success')
    except Exception as e:
        traceback.print_exc()
        sys.stderr.write('dump cjv json failed: %s\n' % outDict)
        hadoop_counter.increase_counter('cjv', 'dump_failed')

if __name__ == "__main__":
    isPush = isProcessPushCJV()
    for line in sys.stdin:
        line = line.strip()
        if line.find('"checked":true,') < 0 and line.find('"push_recved":true') < 0 and line.find('"clicked":true') < 0:
            continue

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue

        try:
            cjvData = json.loads(splits[1])
            process_cjv(cjvData, isPush)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load cjv json failed: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_json')
    hadoop_counter.print_counter()

