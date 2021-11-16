import sys
sys.path.insert(0, '')
import traceback
import time

import utils

def getDateStr():
    env = utils.HadoopEnv()
    if env.is_running_on_hadoop():
        input_path = env.map_input_file
        sys.stderr.write('running on hadoop, input:%s\n' % input_path)
        pathSplits = input_path.split('/')
        return pathSplits[-2]
    else:
        sys.stderr.write('running on local\n')
        return time.strftime('%Y%m%d', time.localtime(time.time()))

if __name__ == "__main__":
    dateStr = getDateStr()
    sys.stderr.write('date str: %s\n' % dateStr)
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue
        print '%s\t%s' % (line, dateStr)

