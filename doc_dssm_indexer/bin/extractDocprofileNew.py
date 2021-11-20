# encoding: utf-8
import io
import os
import sys
import json
import math
import numpy as np

import basic_types
from basic_types import TrainMode,CatVocab,DiscreteVocab,Feature,Impression,Session,Item

config = {
    "feature_list": [
        Feature(name = "r_did", maxLen = 50, vocabName = "docid", isDocSide = False),
        Feature(name = "r_did_100", maxLen = 50, vocabName = "docid", isDocSide = False),
        Feature(name = "u_pos_chns", maxLen = 24, vocabName = "docpoi", isDocSide = False),
        Feature(name = "u_neg_chns", maxLen = 24, vocabName = "docpoi", isDocSide = False),
        Feature(name = "u_pos_tcat", maxLen = 8, vocabName = "docpoi", isDocSide = False),
        Feature(name = "u_neg_tcat", maxLen = 8, vocabName = "docpoi", isDocSide = False),
        Feature(name = "u_pos_tpcm", maxLen = 8, vocabName = "docpoi", isDocSide = False),
        Feature(name = "u_neg_tpcm", maxLen = 8, vocabName = "docpoi", isDocSide = False),

        Feature(name = "docid", maxLen = 1, vocabName = "docid", isDocSide = True),
        Feature(name = "domain", maxLen = 1, vocabName = "domain", isDocSide = True),
        Feature(name = "d_chns", maxLen = 5, vocabName = "docpoi", isDocSide = True),
        Feature(name = "d_tpc_s", maxLen = 3, vocabName = "docpoi", isDocSide = True),
        Feature(name = "d_tpc_m", maxLen = 3, vocabName = "docpoi", isDocSide = True),
        Feature(name = "d_cats", maxLen = 4, vocabName = "docpoi", isDocSide = True),

        Feature(name = "d_clicklevel", vocabName = "clicklevel", isDocSide = True, isContinuous=True),
        Feature(name = "d_ctrlevel", vocabName = "ctrlevel", isDocSide = True, isContinuous=True),
        Feature(name = "bertEmb", maxLen = 64, vocabName = None, isDocSide = True, isContinuous=True),
    ]
}

def readjson(filename):
    try:
        with io.open(filename, encoding='utf-8') as fin:
            content = fin.read()
            return json.loads(content)
    except:
        return {}

def tolower(list):
    return [item.lower() for item in list]

def sortDictByValue(d, reverse=True):
    return [item[0] for item in sorted(d.items(), key=lambda item: item[1], reverse=reverse)]

def hasSubCates(cat, catSet):
    for k in catSet:
        if cat != k and k.startswith(cat):
            return True
    return False

def profileJsonToTrainItem(doc_obj, filename = "", isExp = False, isFilter = False):
    docid = ""
    if "data" not in doc_obj or len(doc_obj["data"].keys()) == 0:
        return "", {}
    docid = list(doc_obj["data"].keys())[0]
    doc_obj = doc_obj["data"][docid]
    if "static_feature" not in doc_obj:
        return "", {}
    static_obj = doc_obj["static_feature"]

    if ("local_score" in static_obj and float(static_obj["local_score"]) > 0.5) and ("geotag" in static_obj and len(static_obj["geotag"]) > 0):
        return "", {}

    d_chns = tolower(static_obj["channels"]) if "channels" in static_obj else []
    raw_d_cats = []
    if "text_category" in static_obj:
        if "third_cat" in static_obj["text_category"]:
            raw_d_cats.extend(tolower(sortDictByValue(static_obj["text_category"]["third_cat"])))
        if "second_cat" in static_obj["text_category"]:
            raw_d_cats.extend(tolower(sortDictByValue(static_obj["text_category"]["second_cat"])))
        if "first_cat" in static_obj["text_category"]:
            raw_d_cats.extend(tolower(sortDictByValue(static_obj["text_category"]["first_cat"])))
    d_cats = []
    for cat in raw_d_cats:
        match = False
        for d_cat in d_cats:
            if len(d_cat) > len(cat) and d_cat.startswith(cat):
                match = True
                break
        if not match:
            d_cats.append(cat)
    d_tpc_s = tolower(sortDictByValue(static_obj["tpc_s"])) if "tpc_s" in static_obj else []
    d_tpc_m = tolower(sortDictByValue(static_obj["tpc_m"])) if "tpc_m" in static_obj else []

    data = {}
    data["docid"] = [docid]
    data["domain"] = [static_obj["domain"]]
    data["d_cats"] = d_cats
    data["d_tpc_s"] = d_tpc_s
    data["d_tpc_m"] = d_tpc_m
    data["d_chns"] = d_chns

    total_view = 0
    total_click = 0
    if "cfb_6h" in doc_obj:
        total_view += doc_obj["cfb_6h"]["view"]
        total_click += doc_obj["cfb_6h"]["click"]
    if "cfb_1h" in doc_obj:
        total_view += doc_obj["cfb_1h"]["view"]
        total_click += doc_obj["cfb_1h"]["click"]
    if "cfb_1d" in doc_obj:
        total_view += doc_obj["cfb_1d"]["view"]
        total_click += doc_obj["cfb_1d"]["click"]
    if "cfb_3d" in doc_obj:
        total_view += doc_obj["cfb_3d"]["view"]
        total_click += doc_obj["cfb_3d"]["click"]
    
    if isFilter and total_click <= 2:
        return "", {}

    if isExp:
        data["d_clicklevel"] = [clickLevel(total_click)]
        data["d_ctrlevel"] = [ctrLevel(total_click, total_view)]
        data["bertEmb"] = normorlizeVec(static_obj["doc_bert_em_reduced"] if "doc_bert_em_reduced" in static_obj else None, 64)
        data["bertEmb"] = [round(x, 4) for x in data["bertEmb"]]

    item = Item(False, data = data, docid=docid)

    for feature in config["feature_list"]:
        if feature.isDocSide:
            if feature.isContinuous:
                if isExp:
                    item.data[feature.name] = data[feature.name]
            else:
                x_f = []
                if feature.name in item.data:
                    value = item.data[feature.name]
                    x_f = value[-feature.maxLen:] if feature.alignRight else value[:feature.maxLen]
                padding = feature.maxLen - len(x_f)
                if padding > 0:
                    x_f = (([feature.defaultValue] * padding) + x_f) if feature.alignRight else (x_f + ([feature.defaultValue] * padding))
                item.data[feature.name] = x_f

    return docid, item

def clickLevel(total_click):
    return math.log(1 + max(0, total_click))

def ctrLevel(total_click, total_view):
    return max(0, min(0.5, (1 + total_click) / (100.0 + total_view)))

def normorlizeVec(vec, dimension):
    if vec == None or len(vec) != dimension:
        vec = [1 for i in range(dimension)]
    sum = min(1, np.linalg.norm(vec))
    return (np.array(vec) / sum).tolist()

class GetHashCode:
    def __init__(self):
        self.cache = {}
        self.power232 = 2**32
        self.power231 = 2**(32-1)
        self.base = [0] * 200
        for i in range(len(self.base)):
            self.base[i] = 31**i

    def getHashCode(self,s):
        h = 0
        n = len(s)
        for i, c in enumerate(s):
            h = h + ord(c)* self.base[n-1-i]
        return (h + self.power231) % self.power232 - self.power231

    def getHashCodeCache(self,s):
        ret = self.cache.get(s)
        if ret == None:
            h = 0
            n = len(s)
            for i, c in enumerate(s):
                h = h + ord(c)* self.base[n-1-i]
            ret = (h + self.power231) % self.power232 - self.power231
            self.cache[s] = ret
        return ret

def toTfservingJson(item, isExp):
    body = {}
    for feature in config["feature_list"]:
        if feature.isDocSide:
            if feature.isContinuous:
                if isExp:
                    body['i_{}'.format(feature.name)] = item.data[feature.name]
            else:
                hash_codes = []
                hash_code_getter = GetHashCode()
                for fea in item.data[feature.name]:
                    hash_codes.append(hash_code_getter.getHashCode(fea))
                body['i_{}'.format(feature.name)] = hash_codes
    ret = {"instances":[body]}
    return ret

if __name__ == "__main__":
    post_file = sys.argv[1]
    post_exp_file = sys.argv[2]
    post_file_filter = sys.argv[3]
    post_exp_file_filter = sys.argv[4]
    json_dir = sys.argv[5]

    f_post = open(post_file, "a")
    f_post_exp = open(post_exp_file, "a")
    f_post_filter = open(post_file_filter, "a")
    f_post_exp_filter = open(post_exp_file_filter, "a")

    for root, dirs, files in os.walk(json_dir):
        for f in files:
            filename = os.path.join(root, f)
            doc_obj = readjson(filename)
            docid, item = profileJsonToTrainItem(doc_obj, filename, False)
            if docid != "":
                f_post.write("%s\t%s\n" % (docid, json.dumps(toTfservingJson(item, False))))

            docid, item = profileJsonToTrainItem(doc_obj, filename, True)
            if docid != "":
                f_post_exp.write("%s\t%s\n" % (docid, json.dumps(toTfservingJson(item, True))))

            # filter
            docid, item = profileJsonToTrainItem(doc_obj, filename, False, True)
            if docid != "":
                f_post_filter.write("%s\t%s\n" % (docid, json.dumps(toTfservingJson(item, False))))

            docid, item = profileJsonToTrainItem(doc_obj, filename, True, True)
            if docid != "":
                f_post_exp_filter.write("%s\t%s\n" % (docid, json.dumps(toTfservingJson(item, True))))

    f_post.close()
    f_post_exp.close()
    f_post_filter.close()
    f_post_exp_filter.close()    

