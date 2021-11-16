import sys

sys.path.insert(0, '')
import json
import traceback
import utils
import copy


hadoop_counter = utils.Counter()


class MergeInfo:
    def __init__(self, user_id):
        self.user_id = user_id
        self.merge_list = []

    def append(self, ctr_info):
        self.merge_list.append(ctr_info)

    def merge(self):
        global hadoop_counter
        if len(self.merge_list) == 0:
            hadoop_counter.increase_counter('merge','incorrect number for merge')
            return None
        
        ret_dict = {}
        for ctr_dict in self.merge_list:
            if len(ctr_dict) == 0:
                continue
            kvs = ctr_dict.items()
            for chn, actions in kvs:
                if chn not in ret_dict:
                    ret_dict[chn] = {}
                for action, value in actions.items():
                    if action not in ret_dict[chn]:
                        ret_dict[chn][action] = 0
                    ret_dict[chn][action] += value
        hadoop_counter.increase_counter('reduce', 'merge_success')
        return ret_dict
        

    def print_output(self, checkThreshold, clickThreshold):
        global hadoop_counter
        merge_info = self.merge()
        if merge_info != None:
            filtered = {k: v for k, v in merge_info.iteritems() if v['check'] >= checkThreshold and v['click'] >= clickThreshold}
            if len(filtered) > 0:
                print '%s\t%s' % (self.user_id, json.dumps(filtered))
                hadoop_counter.increase_counter('reduce', 'output')
            else:
                hadoop_counter.increase_counter('reduce', 'filter')
                sys.stderr.write('filter %s %s\n' % (self.user_id, merge_info))
            
            # print '%s\t%s' % (self.user_id, json.dumps(merge_info))
            # hadoop_counter.increase_counter('reduce', 'output')



if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage python %s check_threshold click_threshold\n" % sys.argv[0])
        sys.exit(1)
    
    checkThreshold = int(sys.argv[1])
    clickThreshold = int(sys.argv[2])
    sys.stderr.write('thresholds check: %d, click: %d\n' % (checkThreshold, clickThreshold))
    
    mergeInfo = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        # click category
        try:
            if mergeInfo is None or mergeInfo.user_id != splits[0]:
                if mergeInfo is not None:
                    mergeInfo.print_output(checkThreshold, clickThreshold)
                mergeInfo = MergeInfo(splits[0])
            mergeInfo.append(json.loads(splits[1]))
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')

    if mergeInfo is not None:
        mergeInfo.print_output(checkThreshold, clickThreshold)
    hadoop_counter.print_counter()
