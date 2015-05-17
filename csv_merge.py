#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        csv_merge
# Purpose:     Merging daily CSV data together
#
# Author:      Irina Maslowski
#
# Created:     07/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

import os,sys,csv
import operator

dirpath = "E:\\Python32\\results_tmp\\"
filedata = dict()

for _i in range(11) : 
    filename = "{:02d}_smiley.csv".format(_i+1)
    with open(os.path.join(dirpath,filename), newline='') as _csvfile : 
        _csvreader = csv.reader(_csvfile, delimiter='\t')
        for _line in _csvreader : 
            _key,_count = _line[0],_line[1]
            if _line[0] not in filedata.keys() :  
                filedata[_key] = 0
            filedata[_key] += int(_count)
            
sorted_data = sorted(filedata.items(), key=operator.itemgetter(1))
sorted_data.reverse()

_filePath = os.path.join(dirpath,"total_smiley.csv")
_outFile = open(_filePath,'w')
# On ecrit les mots et les occurences résultat
for _k,_v in sorted_data :
    _outFile.write('{}\t{}\n'.format(_k,_v))
_outFile.close()

filedata.clear()
fileref = dict()
for _i in range(11) : 
    filename = "{:02d}_liwc.csv".format(_i+1)
    with open(os.path.join(dirpath,filename), newline='') as _csvfile : 
        _csvreader = csv.reader(_csvfile, delimiter=';')
        for _line in _csvreader : 
            _key,_count,_data = _line[0],_line[1],_line[2]
            if _line[0] not in filedata.keys() :  
                filedata[_key] = 0
            filedata[_key] += int(_count)
            if _line[0] not in fileref.keys() :
                fileref[_key] = _line[2]
sorted_data = sorted(filedata.items(), key=operator.itemgetter(1))
sorted_data.reverse()

_filePath = os.path.join(dirpath,"total_liwc.csv")
_outFile = open(_filePath,'w')
# On ecrit les mots et les occurences résultat
for _k,_v in sorted_data :
    _outFile.write('{}\t{}\t{}\n'.format(_k,_v,fileref[_k]))
_outFile.close()
