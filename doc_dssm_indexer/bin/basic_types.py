# encoding: utf-8
from enum import Enum
import collections
import math

TrainMode = Enum('TrainMode', ('Both', 'DocSide', 'UserSide'))
        
class CatVocab:
    #Feature和Vocab是多对一的关系，例如doc中的chn和user侧的pos chn和neg chn可以共用一个vocab，从而共享embedding
    def __init__(self, name, max_num_vocab_tokens = 10, oov_ratio = 0.2, delta_ratio = 0.2, embedding_dims = 32):
        #注意init_num_vocab_tokens - 1 - num_oov_tokens才是实际能够用到的tokens
        self.name = name
        self.counter = collections.Counter()  #底层词表计数器。
        self.max_num_vocab_tokens = max_num_vocab_tokens  #token初始值
        self.num_oov_tokens = math.ceil(max_num_vocab_tokens * oov_ratio)  #oov token个数，在增量训练过程保持不变，默认为max_init_vocab_tokens的20%

        self.delta_vocab_tokens = math.ceil(max_num_vocab_tokens * delta_ratio)  #每轮小时级训练增量token数，默认为max_init_vocab_tokens的20%
        self.embedding_dims = embedding_dims  #embedding维度
    
    def __repr__(self):
        out = []
        out.append("name=" + str(self.name))
        out.append("max_num_vocab_tokens=" + str(self.max_num_vocab_tokens))
        out.append("num_oov_tokens=" + str(self.num_oov_tokens))
        out.append("delta_vocab_tokens=" + str(self.delta_vocab_tokens))
        out.append("embedding_dims=" + str(self.embedding_dims))
        return "<" + " ".join(out) + ">"
    
    def mostCommonWords(self, n):
        return self.counter.most_common(n)
        
    def wordCnt(self):
        return len(self.counter.keys())
    
    def oovRatio(self):
        return 0
    
    def addCorpus(self, corpus):
        pass
    
    def initVocab(self):
        pass
    
    def updateVocab(self):
        #增量更新之后，增加vocab。注意只会新增vocab，旧的word不会退场。
        pass
    

class DiscreteVocab:
    #Feature和Vocab是多对一的关系，例如doc中的chn和user侧的pos chn和neg chn可以共用一个vocab，从而共享embedding
    def __init__(self, name, numofbins = None, embedding_dims = 32):
        #注意init_num_vocab_tokens - 1 - num_oov_tokens才是实际能够用到的tokens
        self.name = name
        #以下属性只用于离散型特征
        self.numofbins = numofbins
        self.embedding_dims = embedding_dims  #embedding维度
        self.bins = []
        self.continuous_vocab = []
    
    def __repr__(self):
        out = []
        out.append("name=" + str(self.name))
        out.append("numofbins=" + str(self.numofbins))
        out.append("embedding_dims=" + str(self.embedding_dims))
        out.append("bins=" + str(self.bins))
        return "<" + " ".join(out) + ">"

class Feature:
    def __init__(self, name, maxLen = 1, alignRight=False, vocabName = None, isDocSide = False, isContinuous = False):
        self.name = name
        self.maxLen = maxLen  #字段截断最大长度
        self.alignRight = alignRight  #False时，字段超过最大长度时左对齐，截断最右边的值，字段不够时右侧加padding；True时右对齐，截断和padding都发生在左侧。
        self.vocabName = vocabName
        self.vocab = None        
        #用户侧还是doc侧，仅用于双塔网络
        self.isDocSide = isDocSide
        #是否是离散值特征。
        self.isContinuous = isContinuous
        if self.isContinuous:
            self.defaultValue = -1
        else:
            self.defaultValue = ''
        
        #feature内部计算方式：meanpooling；位置加权meanpooling；全连接？
        
        #统计值
        self.actualMaxLen = 0   #样本中该字段实际最大长度
        self.avgLen = 0
        self.has_duplicate = False
    
    def __repr__(self):
        out = []
        out.append("name=" + str(self.name))
        out.append("maxLen=" + str(self.maxLen))
        out.append("alignRight=" + str(self.alignRight))
        out.append("vocabName=" + str(self.vocabName))
        out.append("defaultValue=" + str(self.defaultValue))
        out.append("isDocSide=" + str(self.isDocSide))
        out.append("isContinuous=" + str(self.isContinuous))
        return "<" + " ".join(out) + ">"
        
class Impression:
    def __init__(self):
        self.items = []
        self.impid = None
        self.imptime = None
        self.total_click = 0
        self.total_check = 0
        self.has_next_imp = False
        
    def impression_samples(self):
        x = []
        label = self.has_next_imp
        return x, label
    
    def is_valid(self):
        return Treue
        
class Session:
    #sesison切分的作用
    #判断是否用户的刷新之后是否有下一次刷新。
    
    def __init__(self):
        self.imps = []
        self.starttime = None
        self.endtime = None
        self.total_click = 0
        self.total_check = 0
        self.uid = None
        self.status = 0  # 0: 新建，可以增加新的样本，1：超时，新样本会切分成新的session。

class Item:
    def __init__(self, label, data = None, weight = 1.0, anno = None, impid=None, online_score=None, exp=None, docid=None, cond=None):
        self.label = label
        self.weight = weight
        self.data = data
        self.anno = anno
        self.impid = impid
        self.cond = cond
        self.online_score = online_score
        self.exp = exp
        self.docid = docid
    
    def __repr__(self):
        out = []
        out.append("label=" + str(self.label))
        out.append("data=" + str(self.data))
        if self.weight != 1.0:
            out.append("weight=" + str(self.weight))
        out.append("cond=" + str(self.cond))
        out.append("impid=" + str(self.impid))
        out.append("online_score=" + str(self.online_score))
        out.append("docid=" + str(self.docid))
        out.append("exp=" + str(self.exp))
        return "<" + " ".join(out) + ">"
