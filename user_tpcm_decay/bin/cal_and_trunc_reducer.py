import sys

sys.path.insert(0, '')
import json
import traceback
import utils
import copy
from collections import OrderedDict

hadoop_counter = utils.Counter()


class MergeInfo:
    def __init__(self, user_id, fy_coef, limit, tot_to_bot):
        self.user_id = user_id
        self.merge_list = []
        self.fy_coef = fy_coef
        self.limit = limit
        self.tot_to_bot = tot_to_bot

    def append(self, ctr_info):
        self.merge_list.append(ctr_info)

    def trunc(self, data):
        if self.limit <= 0 or len(data) <= self.limit:
            return OrderedDict(sorted(data.items(), key=lambda kv: float(kv[1]), reverse=True))

        bottom = self.limit / self.tot_to_bot
        top = self.limit - bottom
        # FIXME trunc by ctr
        #sortedList = list(sorted(data.items(), key=lambda kv:(kv[1]['c'] / kv[1]['v'], kv[1]['v']), reverse=True))
        sortedList = list(sorted(data.items(), key=lambda kv: float(kv[1]), reverse=True))
        #print sortedList
        out = sortedList[0:top]
        s = max(top, len(sortedList) - bottom)
        out += sortedList[s:]
        out = OrderedDict(out)

        return out

    def merge(self):
        global hadoop_counter
        if len(self.merge_list) == 0 or len(self.merge_list) > 2:
            hadoop_counter.increase_counter('merge','incorrect number of users')
            return None
        
        ret_dict = {}
        coef = 1
        for ctr_dict in self.merge_list:
            if len(ctr_dict) == 0:
                continue
            kvs = ctr_dict.items()
            if 'v' in kvs[0][1]:
                coef = self.fy_coef
            
            for chn, actions in kvs:
                if chn not in ret_dict:
                    ret_dict[chn] = 0
                for action, value in actions.items():
                    if action == 'c':
                        ret_dict[chn] = '%.6f' % (float(value) * coef + float(ret_dict[chn]))
                    elif action == 'v':
                        ret_dict[chn] = '%.6f' % (float(value) * coef * -0.083 + float(ret_dict[chn]))
            coef = 1
        hadoop_counter.increase_counter('merge', 'success')
        return ret_dict
        

    def print_output(self):
        global hadoop_counter
        merge_info = self.merge()
        res = self.trunc(merge_info)
        if res != None:
            print '%s\t%s' % (self.user_id, json.dumps(res))
            hadoop_counter.increase_counter('reduce', 'output')



if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Usage python %s fy_coef, limit, tot_to_bot \n" % sys.argv[0])
        sys.exit(1)
    # Execute Main functionality
    fy_coef = float(sys.argv[1])
    limit = int(sys.argv[2])
    tot_to_bot = int(sys.argv[3])
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
                mergeInfo = MergeInfo(splits[0], fy_coef, limit, tot_to_bot)
            mergeInfo.append(json.loads(splits[1]))
            hadoop_counter.increase_counter('reduce', 'success')
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('load category json failed: %s\n' % splits[1])
            hadoop_counter.increase_counter('reduce', 'invalid_json')

    if mergeInfo is not None:
        mergeInfo.print_output()
    hadoop_counter.print_counter()
