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
    old_zip = None
    old_dict = None
    new_zip = None
    young_dict = None
    for line in sys.stdin:
        #load two dict
        if old_zip == None:
            old_zip = line.split('\t')[0]
            old_dict = json.loads(line.split('\t')[1])
        else:
            new_zip = line.split('\t')[0]
            new_dict = json.loads(line.split('\t')[1])
    for key , value_old in old_dict.items():
        if key in new_dict:
            print(old_zip,key, value_old)
            print(new_zip, key, new_dict[key])
            print()
            new_dict.pop(key, None)
            old_dict.pop(key, None)

    for key, value_old in old_dict.items():
        print(old_zip, key, value_old)
        print()

    for key, value_new in new_dict.items():
        print(new_zip, key, value_new)
        print()

