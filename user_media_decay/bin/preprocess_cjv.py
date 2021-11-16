#input:  uid \t cjv
#output: docid \t uid \t ts \t clicked

import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()

def filter_cjv(item):
    if item is None:
        return False
    if not item.get('joined', False) or not item.get('checked', False):
        return False
    elif len(item.get('user_id', '')) == 0 and len(item.get('docid', '')) == 0:
        return False
    return True

def isClick(cjvData):
    if cjvData.get('thumbed_down', False):
        return False
    elif cjvData.get('clicked', False) and cjvData.get('pv_time', 0) > 2000:
        return True
    #elif cjvData.get('shared', False) or cjvData.get('thumbed_up', False):
    #    return True
    else:
        return False

def process_cjv(cjvData):
    global hadoop_counter
    if not filter_cjv(cjvData):
        hadoop_counter.increase_counter('cjv', 'filter')
        return
    
    uid = cjvData['user_id']
    docid = cjvData['docid']
    ts = cjvData['ts']
    clicked = 1 if isClick(cjvData) else 0
    if clicked == 0 and not cjvData.get('thumbed_down', False) and cjvData.get('cv_time', 0) < 1000:
        hadoop_counter.increase_counter('cjv', 'filter')
        return
    
    outArr = [docid, uid, '%d' % ts,'%d' % clicked]
    print '\t'.join(outArr)
    hadoop_counter.increase_counter('cjv', 'success')

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
