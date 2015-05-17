#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        XMLtoCSV
# Purpose:     XML to CSV transformation
#
# Author:      Irina Maslowski
#
# Created:     07/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

import csv
import os
import xml.etree.ElementTree as ET
import re, html.entities

def xml2csv(path):
    print(path)
    getText = lambda x: '' if not len(x) else x[0].text

    root = None
    try :
        tree = ET.parse(path)
        root = tree.getroot()
    except:
        illegal =  list("&#x{0:x};".format(i) for i in range(32))
        for _i in ["&#x9;","&#xa;","&#xd;",] : illegal.remove(_i)
        _newpath = "tmp.xml"
        _tmpfile = open(_newpath,"w",encoding="utf-8-sig")
        with open(path,encoding="utf-8-sig") as inputdata:
            for _n,_line in enumerate(inputdata) :
                _outstr = _line
                for _chr in illegal :
                    if _line.find(_chr)>= 0:
                        #print(_chr,_n,_line.find(_chr),'\n',_line)
                        _outstr = _outstr.replace(_chr,'')
                        #print(_outstr)
                _tmpfile.write(_outstr)
        _tmpfile.close()
        tree = ET.parse(_newpath)
        root = tree.getroot()

    resultat=list()
    
    for dialog in root.findall('dialog'):
        contextId= dialog.get('contextId')
        dialogId = dialog.find('.id').text
        data=dialog.findall('.//interaction')
        for inter in data:
            dic=dict()
            dic['dialogId']=dialogId
            dic['interactionId']=getText(inter.findall('.//id'))
            dic['talk']=getText(inter.findall('.//userTalk'))
            dic['answer']=getText(inter.findall('.//botAnswer'))
            dic['date']=getText(inter.findall('.//date'))
            dic['contextId']=contextId
            dic['feedback']=getText(inter.findall('.//feedBack'))
            resultat.append(dic)
        externe=dialog.findall('.//externValue')
        for _ext in externe :
            _current = getText(_ext.findall('.//name'))
            if _current == 'civilite' :
                dic['civilite']= getText([ _ext.find('.//value'),] )
                break
            
    print('\tParse OK')
    filename = os.path.basename(path).replace('.','_')
    import codecs
    output_file=codecs.open(filename+'.csv','w','utf-8')
    fieldnames = ['dialogId','interactionId','feedback']
    writer = csv.DictWriter(output_file,fieldnames=fieldnames,delimiter='|',lineterminator='\n')
    writer.writeheader()
    for row in resultat :
        _tmp = dict.fromkeys(fieldnames)
        for field in fieldnames :
            if row.get(field,None) is None :
                _tmp[field] = row.get(field,None)
            else : 
                _tmp[field] = row[field]#.encode('utf-8')
        writer.writerow(_tmp)
    output_file.close()
    print('\tCSV OK')

SOLO = False
if SOLO : 
    _errfile="D:\\2014\\102014\\04-10-2014.xml"
    xml2csv(_errfile)
else : 
    _dir = 'D:\\2014\\'
    errorlog=open('error.txt','w')
    for _roots,_indirs,_paths in os.walk(_dir):
        for _filename in _paths :
            try:
                xml2csv(os.path.join(_roots,_filename))
                #print(os.path.join(_roots,_filename) )
            except Exception as E:
                    errorlog.write(os.path.join(_roots,_filename))
                    errorlog.write(str(E))
                    errorlog.write('\n')

    errorlog.close()
    xml2csv(os.path.join(_roots,_filename) )

