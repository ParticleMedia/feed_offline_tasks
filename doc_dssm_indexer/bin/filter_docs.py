import sys
import json
import traceback
import time

sys.path.insert(0, '')

PIVOT_DAYS = 3

def isValid(docData, pivot, block_mids):
    if '_id' not in docData:
        return False

    # disable index
    if docData.get('disable_index', False):
        return False

    # staleness
    epoch = docData.get('epoch', -1)
    if epoch < pivot:
        return False

    if 'Obituary' in docData.get('text_category', {}).get('first_cat', {}):
        return False

    # local news
    isLocalNews = docData.get('local_score', 0.0) > 0.5
    if isLocalNews:
        return False

    # blocked media ids
    #mid = docData.get('media_id', '')
    #if mid in block_mids:
    #    return False

    if docData.get('ctype') != 'native_video':
        return False

    return True

if __name__ == "__main__":
    block_mids = {}
    #f = open("/mnt/models/foryou/native_video_mid_block_list.txt")
    #line = f.readline()
    #while line:
    #    block_mids[line] = True
    #    line = f.readline()
    #f.close()

    pivot = time.time() - 3600 * 24 * PIVOT_DAYS
    for line in sys.stdin:
        line = line.strip()

        docData = json.loads(line)
        if isValid(docData, pivot, block_mids):
            print docData['_id']

