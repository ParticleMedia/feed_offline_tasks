#input:  uid \t cjv
#output: docid \t uid \t ts \t clicked

import sys
sys.path.insert(0, '')
import json
import traceback

import utils

hadoop_counter = utils.Counter()


FILTER_CHNS = {'Star','Instagram'}

def filter_cjv(data,clickThreshold, ctrThreshold):
    if data is None:
        return None
    ret_dict={}
    for k, v in data.items():
        if k in FILTER_CHNS or float(v['c']) < clickThreshold or float(v['c']) / float(v['v']) < ctrThreshold:
            continue
        ret_dict[k] = v
    if len(ret_dict) < 2:
        return None
    if len(ret_dict) > 20:
        ret_dict = dict(sorted(ret_dict.items(), key=lambda x: float(x[1]['c'])-0.083*float(x[1]['v']), reverse=True)[:20])
    total_click = 0.0
    for k, v in ret_dict.items():
        total_click += float(v['c'])
    ret_dict = {k: {'tf': round(float(v['c']) / total_click, 6), 'c':round(float(v['c']), 6)} for k, v in ret_dict.iteritems()} 
    return ret_dict

def process_line(user_id, data, clickThreshold, ctrThreshold):
    global hadoop_counter
    data = filter_cjv(data, clickThreshold, ctrThreshold)
    if data is None:
        hadoop_counter.increase_counter('cjv', 'filter')
        return
    outArr = [user_id, json.dumps(data)]
    print '\t'.join(outArr)
    hadoop_counter.increase_counter('cjv', 'success')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s click_threshold ctr_threshold\n" % sys.argv[0])
        sys.exit(1)
    
    clickThreshold = float(sys.argv[1])
    ctrThreshold = float(sys.argv[2])
    sys.stderr.write('thresholds click: %f, ctr: %f\n' % (clickThreshold, ctrThreshold))

    for line in sys.stdin:
        line = line.strip()
        splits = line.split('\t')
        if len(splits) < 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue
        try:
            data = json.loads(splits[1])
            process_line(splits[0], data, clickThreshold, ctrThreshold)

        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load cjv json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('cjv', 'invalid_json')
    hadoop_counter.print_counter()
