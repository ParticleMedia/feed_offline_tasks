#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0, '')

MIN_PV_TIME = 2000
MAX_PV_TIME = 600000
MIN_CV_TIME = 2000
MAX_CV_TIME = 600000

class Action:
    def __init__(self, splits):
        self.pv_time=0
        self.cv_time=0
        self.checked=int(splits[0])
        self.clicked=int(splits[3])
        self.liked=int(splits[4])
        self.shared=int(splits[5])
        self.thumbed_up=int(splits[6])
        self.thumbed_down=int(splits[7])

        if len(splits[1]) > 0 and splits[1] != '\N':
            self.pv_time=int(splits[1])
        if len(splits[2]) > 0 and splits[2] != '\N':
            self.cv_time=int(splits[2])
    
    def isClick(self):
        return self.clicked > 0 and (self.pv_time == 0 or self.pv_time >= MIN_PV_TIME) and self.thumbed_down == 0

    def isValidCheck(self):
        if self.clicked > 0 or self.liked > 0 or self.shared > 0 or self.thumbed_up > 0 or self.thumbed_down > 0:
            return True
        else:
            return self.cv_time >= MIN_CV_TIME or self.cv_time == 0

    def normalize(self):
        # must normalize click firstly
        self.clicked = 1 if self.isClick() else 0
        if self.pv_time > MAX_PV_TIME:
            self.pv_time = 0
        if self.cv_time > MAX_CV_TIME:
            self.cv_time = 0

    def combine(self, other):
        if other.pv_time <= MAX_PV_TIME:
            self.pv_time += other.pv_time
        if other.cv_time <= MAX_CV_TIME:
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

def parseAction(splits):
    if len(splits) <= 7:
        return None
    else:
        return Action(splits)

class ActionAgg:
    def __init__(self):
        self.pv_time=0
        self.cv_time=0
        self.check=0
        self.click=0
        self.like=0
        self.share=0
        self.thumbed_up=0
        self.thumbed_down=0

    def accumulate(self, action, ts):
        # TODO time decay by ts
        self.check += action.checked
        self.pv_time += action.pv_time
        self.cv_time += action.cv_time
        self.click += action.clicked
        self.like += action.liked
        self.share += action.shared
        self.thumbed_up += action.thumbed_up
        self.thumbed_down += action.thumbed_down

    def toDict(self):
        d = {
            'v': self.check,
            'c': self.click,
            'cvt': self.cv_time,
            'pvt': self.pv_time,
        }
        if self.like > 0:
            d['l'] = self.like
        if self.share > 0:
            d['s'] = self.share
        if self.thumbed_up > 0:
            d['thu'] = self.thumbed_up
        if self.thumbed_down > 0:
            d['thd'] = self.thumbed_down
        return d

