#input:  uid \t docid \t ts \t docInfo \t clicked ...
#output: uid \t docid \t ts \t category list \t checked \t clicked ...

import sys
sys.path.insert(0, '')
import time
import traceback
import json
import utils
import action

def getTimestampOfToday():
    t = time.localtime(time.time())
    ts = time.mktime(time.strptime(time.strftime('%Y-%m-%d 00:00:00', t),'%Y-%m-%d %H:%M:%S'))
    return int(ts)

def normalize_sc(scs):
    return [round(sc / scs[0] , 8) for sc in scs]

def main(min_ts, field, score_field, filter_logic):
    hadoop_counter = utils.Counter()
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        
        splits = line.split('\t')
        if len(splits) < 5:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('cjv', 'invalid_line')
            continue

        try:
            ts = int(splits[2])
            if ts <= min_ts:
                hadoop_counter.increase_counter('map', 'filter_by_ts')
                continue

            docInfo = json.loads(splits[3])
            if field not in docInfo or len(docInfo.get(field, {})) == 0:
                hadoop_counter.increase_counter('map', 'no_category')
                continue
            if score_field != "None" and (score_field not in docInfo or len(docInfo.get(score_field, [])) == 0 or \
                len(docInfo.get(score_field, [])) != len(docInfo.get(field, []))):
                hadoop_counter.increase_counter('map', 'no_category')
                continue    


            # if field == 'nlu_tags_scores':
            #     if 'nlu_tags'not in docInfo or len(docInfo.get('nlu_tags', [])) == 0 \
            #             or len(docInfo.get('nlu_tags', [])) != len(docInfo.get(field, [])):
            #         hadoop_counter.increase_counter('map', 'no_category')
            #         continue

            actionSplits = ['1']
            actionSplits.extend(splits[4:])
            if len(actionSplits) <= 7:
                act = None
            else:
                act = action.parseAction(actionSplits, filter_logic)
            act.normalize()
            if not act.isValidCheck():
                hadoop_counter.increase_counter('map', 'filter_by_cvtime')
                continue

            #ADD
            maxScore = docInfo[field].items()[0][1]
            for key, value in docInfo[field].items():
                maxScore = max(maxScore, value)

            for key, value in docInfo[field].items():
                docInfo[field][key] = value / maxScore
            
            if score_field != 'None':
                outSplits = [splits[0], splits[1], splits[2],
                             json.dumps(tuple(zip(docInfo[field],normalize_sc(docInfo[score_field]))))]
            else:
                outSplits = [splits[0], splits[1], splits[2], json.dumps(docInfo[field])]

            outSplits.extend(act.toList())
            print ('\t'.join(outSplits))
            hadoop_counter.increase_counter('map', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('map', 'parse_line_fail')
    hadoop_counter.print_counter()

if __name__ == "__main__":
    if len(sys.argv) < 8:
        sys.stderr.write("Usage python %s filter_days field score_field min_pv_time max_pv_time min_cv_time max_cv_time\n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    filter_days = int(sys.argv[1])
    field = sys.argv[2]
    score_field = sys.argv[3]
    min_pv_time = int(sys.argv[4])
    max_pv_time = int(sys.argv[5])
    min_cv_time = int(sys.argv[6])
    max_cv_time = int(sys.argv[7])
    filter_logic = [min_pv_time, max_pv_time, min_cv_time, max_cv_time]
    min_ts = getTimestampOfToday() - 3600 * 24 * filter_days
    sys.stderr.write('min ts: %d\n' % min_ts)
    main(min_ts, field, score_field, filter_logic)
