import httplib
import json
import socket
import sys

SERVER_HOST = 'docenter.ha.nb.com'
SERVER_PORT = 8010
SERVER_IP = socket.gethostbyname(SERVER_HOST)

def getDocFeature(docid, fields):
    url = "/docenter/ids/%s?fields=_id,%s" % (docid, fields)
    conn = httplib.HTTPConnection(SERVER_IP, SERVER_PORT, timeout=10)
    conn.request(method="GET",url=url) 
    response = conn.getresponse()
    res = response.read()
    
    resJson = json.loads(res)
    if len(resJson) == 0 or '_id' not in resJson[0]:
        return None
    else:
        return resJson[0]

def getDocListFeature(docids, fields):
    url = "/docenter/ids/%s?fields=_id,%s" % (','.join(docids), fields)
    conn = httplib.HTTPConnection(SERVER_IP, SERVER_PORT, timeout=10)
    conn.request(method="GET",url=url) 
    response = conn.getresponse()
    res = response.read()
    
    resJson = json.loads(res)
    res_dict = {}
    for item in resJson:
        if '_id' not in item:
            continue 
        id = item['_id']
        res_dict[id] = item
    return res_dict

if __name__ == "__main__":
    sys.stderr.write('ip: %s\n' % SERVER_IP)
    data = getDocListFeature(['0Xoksa1s', '0XuuPfC6'], 'text_category')
    print data
