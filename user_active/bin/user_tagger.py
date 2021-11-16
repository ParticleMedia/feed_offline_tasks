import sys
sys.path.insert(0, '')
import traceback
import json

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if len(line) == 0:
            continue

        splits = line.split('\t')
        if len(splits) < 2:
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')
            continue

        try:
            uid = splits[0]
            userData = json.loads(splits[1])
            outData = [uid]
            outData.append('%d' % userData.get('foryou', {}).get('check_days', 0))
            outData.append('%d' % userData.get('foryou', {}).get('click_days', 0))
            outData.append('%d' % userData.get('foryou', {}).get('checks', 0))
            outData.append('%d' % userData.get('foryou', {}).get('clicks', 0))
            outData.append('%d' % userData.get('local', {}).get('check_days', 0))
            outData.append('%d' % userData.get('local', {}).get('click_days', 0))
            outData.append('%d' % userData.get('local', {}).get('checks', 0))
            outData.append('%d' % userData.get('local', {}).get('clicks', 0))
            outData.append('%d' % userData.get('push', {}).get('check_days', 0))
            outData.append('%d' % userData.get('push', {}).get('click_days', 0))
            outData.append('%d' % userData.get('push', {}).get('checks', 0))
            outData.append('%d' % userData.get('push', {}).get('clicks', 0))
            print '\t'.join(outData)
        except Exception as e:
            traceback.print_exc()
            sys.stderr.write('invalid line: %s\n' % line)
            hadoop_counter.increase_counter('reduce', 'invalid_line')


