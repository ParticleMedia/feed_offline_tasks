# encoding: utf-8
import urllib
import os
import sys
import json

output_dir = sys.argv[1]
for line in sys.stdin:
    line = line.strip()
    if len(line) == 0:
        continue
    url = "http://tools.n.newsbreak.com/doc-profile/get?from=test&docids=" + line + "&fields=static_feature.*,cfb_1d.*,cfb_1h.*,cfb_3d.*,cfb_6h.*"
    output_file = output_dir + "/" + line + ".json"
    os.mknod(output_file)
    urllib.urlretrieve(url, output_file)

