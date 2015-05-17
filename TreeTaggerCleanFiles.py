#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        TreeTaggerCleanFiles
# Purpose:     formating TreeTagger output files and inserting into DB
#
# Author:      Irina Maslowski
#
# Created:     13/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

import os,sys,re,csv
import psycopg2
import time,datetime
TIMENOW        = lambda : datetime.datetime.now().time().strftime('%H:%M:%S')

CNXN_DEBUG      = {'database':'laura_test', 'user':'postgres', 'password':'admin', 'port':"5433"}
CNXN_RELEASE    = {'database':'laura',      'user':'postgres', 'password':'admin', 'port':"5433"}

class TreeTaggerCleaner(object):
    def __init__(self,month):

        filename_in  = "Results//ttag_talk_clean_{:02d}.txt".format(month)
        filename_out = "Results//flat_ttag_talk_clean_{:02d}.txt".format(month)
        self.FILE_IN  = open(filename_in ,'r',encoding='utf-8-sig')
        self.FILE_OUT = open(filename_out,'w',encoding='utf-8-sig',newline='')
        self.outwriter = csv.writer(self.FILE_OUT, delimiter=';', quotechar='"',quoting=csv.QUOTE_ALL)
        self.currentId           = None
        self.currentInteraction  = list()
    
    def clean_current(self):
        # On verifie que l'on retrouve bien les caractères de fin de ligne
        for _i in range(-5,0):
            assert self.currentInteraction[_i] == '--\tPUN\t--',self.currentId
        self.currentInteraction= self.currentInteraction[:-5]
        
        # On retire les marque type '[URL], [MAIL], etc'
        _prune=list()    
        _tags = ['URL','MAIL','DATE','ASP',]
        for _i in range(len(self.currentInteraction)-2):
            # Recherche d'un début de marque
            if self.currentInteraction[_i] == '[\tPUN\t[' and\
                self.currentInteraction[_i+2] == ']\tPUN\t]' and\
                self.currentInteraction[_i+1].split("\t")[0] in _tags : 
                _prune.extend([_i,_i+1,_i+2])
            if self.currentInteraction[_i].startswith('/ASPFront/com/'):
                _prune.extend([_i,_i+1,_i+2,_i+3])
        # On ne garde que les éléments à conserver
        _tmplist = list()
        for _i,_v in enumerate(self.currentInteraction):
            if _i not in _prune : 
                _tmplist.append(_v)
        self.currentInteraction = _tmplist
        for _i in range(len(self.currentInteraction)):
            self.currentInteraction[_i]=self.currentInteraction[_i].replace('|','/')
            
    def run(self):
        _startRe = "###([0-9]+)###.*$"
        _start   = re.compile(_startRe)
        for _line in self.FILE_IN : 
            # Recherche du début d'interaction
            _match = _start.search(_line)
            if _match is not None : 
                if self.currentId is not None :
                    self.clean_current()
                    # On dumpe le match précédent
                    if len(self.currentInteraction) : 
                        self.outwriter.writerow([self.currentId,'|'.join(self.currentInteraction),])
                del self.currentInteraction[:]
                # Interaction trouvée
                self.currentId = _match.group(1)
            else :
                _value = _line.replace('\n','')
                self.currentInteraction.append(_value) 
                    
        self.FILE_IN.close()
        self.FILE_OUT.close()

class InsertTTIntoDB(object):
    def __init__(self,debug=True,month=10):
        # Création de la connection
        if debug : 
            self.cnxn   = psycopg2.connect(**CNXN_DEBUG) 
        else : 
            self.cnxn   = psycopg2.connect(**CNXN_RELEASE) 
        # Ouverture du fichier
        filename_in = "Results//flat_ttag_talk_clean_{:02d}.txt".format(month)
        self.FILE_IN = open(filename_in,'r',encoding='utf-8-sig',newline='')
        self.inreader = csv.reader(self.FILE_IN, delimiter=';', quotechar='"',)
        _cursor = self.cnxn.cursor()
        # Parcours des lignes
        for _row in self.inreader:
            _id,_date = _row
            _cursor.execute(self.getQuery(),(_date,_id,))
        # Fermeture du fichier et des curseurs
        self.cnxn.commit()
        self.cnxn.close()
        self.FILE_IN.close()
    def getQuery(self):
        return "UPDATE lines SET tree_tagger=(%s) WHERE interactionid =(%s)"

for _mon in range(2,12):        
    # print('Tagging',_mon)
    # bob = TreeTaggerCleaner(_mon)
    # bob.run()
    # del bob
    print(TIMENOW(),'Updating',_mon)
    InsertTTIntoDB(False,_mon)     
'''
bob = TreeTaggerCleaner(10)
bob.run()
InsertTTIntoDB(True,10)     
'''
