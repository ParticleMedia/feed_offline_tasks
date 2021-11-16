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
        if (len(self.merge_list) == 1):
            return self.merge_list[0]
        elif (len(self.merge_list) == 2):
            #merge short dict to long dict
            if len(self.merge_list[0]) > len(self.merge_list[1]):
                long_ctr_dict = self.merge_list[0]
                short_ctr_dict = self.merge_list[1]
            else:
                long_ctr_dict = self.merge_list[1]
                short_ctr_dict = self.merge_list[0]

            for chn, actions in short_ctr_dict.items():
                if chn not in long_ctr_dict:
                    long_ctr_dict[chn] = copy.deepcopy(short_ctr_dict[chn])
                else:
                    for action, value in actions.items():
                        if action not in long_ctr_dict[chn]:
                            long_ctr_dict[chn][action] = round(value, 6)
                        else:
                            long_ctr_dict[chn][action] = round(float(value) + float(long_ctr_dict[chn][action]), 6)
            hadoop_counter.increase_counter('merge', 'success')
            return {k: v for k, v in long_ctr_dict.items() if v.get('v', 0.0) > 1e-6}
        hadoop_counter.increase_counter('merge','incorrect number of users')
        return None


    def print_output(self):
        global hadoop_counter
        merge_info = self.merge()
        if merge_info != None:
            print '%s\t%s\t%s' % (self.user_id, json.dumps(merge_info), len(merge_info))
            hadoop_counter.increase_counter('reduce', 'output')
            hadoop_counter.increase_counter('reduce', 'poi_cnt', len(merge_info))



if __name__ == "__main__":
    mergeInfo = None
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) != 3:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        # click category
        try:
            if mergeInfo is None or mergeInfo.user_id != splits[0]:
                if mergeInfo is not None:
                    mergeInfo.print_output()
                mergeInfo = MergeInfo(splits[0])
            mergeInfo.append(json.loads(splits[1]))
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')

    if mergeInfo is not None:
        mergeInfo.print_output()
    hadoop_counter.print_counter()
