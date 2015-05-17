#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        MachineLearning.py
# Purpose:     Method for Machine Learning
#
# Author:      Irina Maslowski
#
# Created:     17/05/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

from sklearn.feature_extraction.text import TfidfVectorizer,CountVectorizer
from sklearn.cluster import KMeans
from sklearn.metrics import precision_score

import matplotlib.pyplot as plt
import matplotlib.cm as cm

import numpy as np

import time,datetime

TIMENOW        = lambda : datetime.datetime.now().time().strftime('%H:%M:%S')

lines = list()
predict = list()
n_clusters = 8

print(TIMENOW(),'LOADING','-'*42)      
with open('output.txt','r',encoding='utf-8-sig') as FILE_IN :
    _readlines = list()
    for _line in FILE_IN: 
        _readlines.append(_line)

lines   =  _readlines[:5000]
predict =  _readlines[5000:]     
        
def run(lines,vectorizerCls):

    print(TIMENOW(),'VECTORIZE','-'*42)      
    vectorizer=vectorizerCls(stop_words=['le','de','la','les','je','un','une','des','est','et','il','elle','du','ai','au',])
    data =vectorizer.fit_transform(lines)
    num_samples, num_features = data.shape
    print("#samples: %d, #features: %d" % (num_samples, num_features)) #samples: 5, #features: 25 #samples: 2, #features: 37
    print(TIMENOW(),'KMEANS','-'*42)      
    km   =KMeans(n_clusters=n_clusters)
    res  =km.fit_transform(data)
    labels = km.labels_
    labels_shape = km.labels_.shape
    print ("labels : ", labels)
    print ("labels_shape : ", labels_shape)

    print(TIMENOW(),'DONE','-'*42)  
        
    print("Top terms per cluster:")
    order_centroids = km.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names()
    result = dict()
    for i in range(n_clusters):
        result[i]=list()
        print("Cluster %d:" % i, end='')
        for ind in order_centroids[i, :25]:
            print(' %s' % terms[ind], end='\n')
            result[i].append(terms[ind])
        print()    
    return result
    
_tfid = run(lines,TfidfVectorizer)
_count = run(lines,CountVectorizer)

for _kt,_vt in _tfid.items():
    print(_kt,':\n\t',end='')
    for _kc,_vc in _count.items():
        _inter = set(_vt).intersection(set(_vc))
        print('{}:#{}, {}'.format(_kc,len(_inter),_inter) )
    print('')


