#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, '')

# MIN_PV_TIME = 2000
# MAX_PV_TIME = 600000
# MIN_CV_TIME = 3800
# MAX_CV_TIME = 600000


class Action:
    def __init__(self, splits, filter_logic):
        self.pv_time=0
        self.cv_time=0
        self.checked=int(splits[0])
        self.clicked=int(splits[3])
        self.liked=int(splits[4])
        self.shared=int(splits[5])
        self.thumbed_up=int(splits[6])
        self.thumbed_down=int(splits[7])
        self.MIN_PV_TIME = int(filter_logic[0])
        self.MAX_PV_TIME = int(filter_logic[1])
        self.MIN_CV_TIME = int(filter_logic[2])
        self.MAX_CV_TIME = int(filter_logic[3])

        if len(splits[1]) > 0 and splits[1] != '\N':
            self.pv_time=int(splits[1])
        if len(splits[2]) > 0 and splits[2] != '\N':
            self.cv_time=int(splits[2])
    
    def isClick(self):
        return self.clicked > 0 and (self.pv_time== 0 or self.pv_time >= self.MIN_PV_TIME) and self.thumbed_down == 0

    def isValidCheck(self):
        if self.clicked > 0 or self.liked > 0 or self.shared > 0 or self.thumbed_up > 0 or self.thumbed_down > 0:
            return True
        else:
            return self.cv_time >= self.MIN_CV_TIME

    def normalize(self):
        # must normalize click firstly
        self.clicked = 1 if self.isClick() else 0
        if self.pv_time > self.MAX_PV_TIME:
            self.pv_time = 0
        if self.cv_time > self.MAX_CV_TIME:
            self.cv_time = 0

    def combine(self, other):
        if other.pv_time <= self.MAX_PV_TIME:
            self.pv_time += other.pv_time
        if other.cv_time <= self.MAX_CV_TIME:
            self.cv_time += other.cv_time
        self.checked += other.checked
        self.clicked += other.clicked
        self.liked += other.liked
        self.shared += other.shared
        self.thumbed_up += other.thumbed_up
        self.thumbed_down += other.thumbed_down

    
    def toList(self):
        out = []
        out.append('%d' % self.checked)
        out.append('%d' % self.pv_time)
        out.append('%d' % self.cv_time)
        out.append('%d' % self.clicked)
        out.append('%d' % self.liked)
        out.append('%d' % self.shared)
        out.append('%d' % self.thumbed_up)
        out.append('%d' % self.thumbed_down)
        return out

    def toString(self):
        out = []
        out.append('%d' % self.checked)
        out.append('%d' % self.pv_time)
        out.append('%d' % self.cv_time)
        out.append('%d' % self.clicked)
        out.append('%d' % self.liked)
        out.append('%d' % self.shared)
        out.append('%d' % self.thumbed_up)
        out.append('%d' % self.thumbed_down)
        return '\t'.join(out)

def parseAction(splits,filter_logic):
    if len(splits) <= 7:
        return None
    else:
        if len(filter_logic) == 0:
            return Action(splits,[2000,600000,2000,600000])
        else:
            return Action(splits,filter_logic)

class ActionAgg:
    def __init__(self,unitDecayCoef):
        self.pv_time=0
        self.cv_time=0
        self.check=0
        self.click=0
        self.like=0
        self.share=0
        self.thumbed_up=0
        self.thumbed_down=0
        if unitDecayCoef:
            self.decay_coef = 1.0
        else:
            self.decay_coef = pow(0.5,(1.0/30))

    def accumulate(self, action, ts):
        self.check += action.checked
        self.pv_time += action.pv_time
        self.cv_time += action.cv_time
        self.click += action.clicked
        self.like += action.liked
        self.share += action.shared
        self.thumbed_up += action.thumbed_up
        self.thumbed_down += action.thumbed_down

    def accumulate_ts_decay(self, cate_score, action, ts, today_t):
        # TODO time decay by ts
        decay = pow(self.decay_coef,(today_t - ts) / (3600*24)) * cate_score
        #decay = cate_score
        self.check += action.checked * decay
        self.pv_time += (action.pv_time * decay)
        self.cv_time += (action.cv_time *decay)
        self.click += action.clicked * decay
        self.like += action.liked * decay
        self.share += action.shared * decay
        self.thumbed_up += action.thumbed_up * decay
        self.thumbed_down += action.thumbed_down * decay

    def toDict(self):
        d = {
            'v': round(self.check, 6),
            'c': round(self.click, 6),
        }
        if self.like > 1e-6:
            d['l'] = round(self.like, 6)
        if self.share > 1e-6:
            d['s'] = round(self.share, 6)
        if self.thumbed_up > 1e-6:
            d['thu'] = round(self.thumbed_up, 6)
        if self.thumbed_down > 1e-6:
            d['thd'] = round(self.thumbed_down, 6)
        return d

