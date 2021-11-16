#!/usr/bin/env python

import sys
import json

reload(sys)
sys.setdefaultencoding('utf8')

for line in sys.stdin:
  line = line.strip()
  splits = line.split(',')
  if len(splits) != 2:
    continue
  user_id = splits[0]
  media_ids = splits[1].split('|')
  if len(media_ids) == 0:
    continue
  print user_id + '\t' + str(json.dumps(media_ids))

