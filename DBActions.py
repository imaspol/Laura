#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        DBActions
# Purpose:     General actions performed in DB
#
# Author:      Irina Maslowski
#
# Created:     07/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------
import os,sys,re,csv
import psycopg2
import time,datetime
from reference_search import ReferenceSearch, SmileySearch, LiwcSearch

STRTIMEtoEPOCH = lambda x:time.mktime(time.strptime(x,"%Y-%m-%d %H:%M:%S.0"))
TIMENOW        = lambda : datetime.datetime.now().time().strftime('%H:%M:%S')

class lineData(object):
    def __init__(self,strdate,dialogId,interactionId,talk):
        self.date           = STRTIMEtoEPOCH(strdate)
        self.dialogId       = dialogId
        self.interactionId  = interactionId
        self.talk           = talk
        self.duplicate      = 0
    
    def compare(self,other):
        if other.talk == self.talk : 
            self.duplicate = other.interactionId
            return True
        return False
        
# ---------------------------------------------------------------------------------------------------------------------
#                                   DBAction
# ---------------------------------------------------------------------------------------------------------------------        

CNXN_DEBUG      = {'database':'laura_test', 'user':'postgres', 'password':'admin', 'port':"5433"}
CNXN_RELEASE    = {'database':'laura',      'user':'postgres', 'password':'admin', 'port':"5433"}
class DBAction(object):
    def __init__(self,debug=True):
        print('\n',self.__class__.__name__)
        # Création de la connection
        if debug : 
            self.cnxn   = psycopg2.connect(**CNXN_DEBUG) 
        else : 
            self.cnxn   = psycopg2.connect(**CNXN_RELEASE) 
        # Création du curseur pour la requête principale
        _cursor = self.cnxn.cursor("getter")
        
        print(TIMENOW(),'Executing Query','-'*30)
        # Exécution de la requête principale
        _cursor.execute(self.getQuery())
        print(TIMENOW(),'Loop','-'*41)
        # On itère sur les résultats de la requête
        for _res in _cursor:
            self.loop(_res)
            
        print(TIMENOW(),'EOL','-'*42)        
        # On applique les derniers traitements nécessaires avant la fermeture
        self.finalize()
        print(TIMENOW(),'Finalize','-'*37)
        # Fermeture de la connection
        self.cnxn.commit()
        self.cnxn.close()
        print('\a'*5) #Beep
        
    def loop(self,data):
        """ Boucle de traitement principale"""
        pass
        
    def finalize(self):
        """ Traitement finaux """
        pass
    
    def getQuery(self):
        """ Requête principale """
        raise NotImplementedError
        
# ---------------------------------------------------------------------------------------------------------------------
#                                   CLASSES FILLES
# ---------------------------------------------------------------------------------------------------------------------
        
class DuplicateAction(DBAction):
    def __init__(self,debug=True):
        self._currentdialog = None
        self._lines = list()
        DBAction.__init__(self,debug)
        
    def getQuery(self):
        return "SELECT date,dialogId,interactionId,talk FROM lines ORDER BY dialogId"
        
    def finalize(self):
        if len(self._lines) :
            self.checkDuplicates()
            
    def updateDB(self,line):
        _query="UPDATE lines SET duplicate=(%s) WHERE interactionid =(%s)"
        _ncursor = self.cnxn.cursor()
        _ncursor.execute(_query,(line.duplicate,line.interactionId) )
        
    def checkDuplicates(self):
        # Tri des lignes
        self._lines.sort(key=lambda x:x.date)
        # On compare les lignes
        for _n,_l in enumerate(self._lines) : 
            for _i in range(_n):
                if _l.compare(self._lines[_i]) : break
        # On met à jour la DB   
        for _l in self._lines :       
            self.updateDB(_l)
        # On nettoie la liste pour limiter la place en mémoire            
        del self._lines[:]  
            
    def loop(self,data):
        _date,_dialogId,_interactionId,_talk = data
        if self._currentdialog != _dialogId : 
            # On a fini le dialogue, on charge les résultats
            self.checkDuplicates()
            self._currentdialog = _dialogId
        # On ajoute la première nouvelle ligne
        self._lines.append(lineData(strdate=_date,dialogId=_dialogId,interactionId=_interactionId,talk=_talk))

class CountAnswerWordAction(DBAction): 
    """ Nettoie la réponse de Laura et compte le nombre de mots """
    def getQuery(self):         return "SELECT interactionId,answer FROM lines"
    def getQueryUpdate(self):   return "UPDATE lines SET answer_clean=(%s), answer_words=(%s), answer_wordcount=(%s),answer_empty=(%s), url=(%s) WHERE interactionid =(%s)"
    
    def clean(self,value):
        # Regex pour éliminer les URL
        re_answer= re.compile("[|]R[|]",re.IGNORECASE)
        re_url   = re.compile("http[s]?://[^:/]+(?::\d+)?(?:/[^?]+)?(?:\?[^#]+)?(?:#.+)?",re.IGNORECASE)
        re_asp   = re.compile("/ASPFront/.*=DF",re.IGNORECASE)
        re_href  = re.compile("<a href=[^>]*>",re.IGNORECASE)
        re_tags  = re.compile("<[^>]*>",re.IGNORECASE)
        # Regex pour éliminer les adresse mail
        re_mail  = re.compile("[\w.-]+@[\w-]+\.\w{2,6}",re.IGNORECASE)
        # Regex pour éliminer les dates
        re_date1 = re.compile("(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])/(19|20)\d\d")
        re_date2 = re.compile("(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])/\d\d")
        re_date3 = re.compile("(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[012])")
        # Applications des Regex
        _newline = re.sub(re_answer,"",value)
        _newline = re.sub(re_href,"\t[URL]\t",_newline)
        _newline = re.sub(re_tags,"",_newline)
        _newline = re.sub(re_url,"\t[URL]\t",_newline)
        _newline = re.sub(re_asp,"\t[ASP]\t",_newline)
        _newline = re.sub(re_mail,"\t[MAIL]\t",_newline)
        _newline = re.sub(re_date1,"\t[DATE]\t",_newline)
        _newline = re.sub(re_date2,"\t[DATE]\t",_newline)
        _newline = re.sub(re_date3,"\t[DATE]\t",_newline)
        return _newline
    
    def loop(self,data):
        _interactionId,_talk = data
        _clean      = self.clean(_talk)
        _words      = re.split('\W+', _clean, flags = re.UNICODE)
        _words      = list(filter(('').__ne__,_words))
        _url        = _words.count('URL')
        _wordcount  = len(_words)
        _empty      = not len(_words) and not _clean.rstrip()
        _ncursor = self.cnxn.cursor()
        _words = '|'.join(_words)
        _ncursor.execute(self.getQueryUpdate(),(_clean,_words,_wordcount,_empty,_url,_interactionId) )

class RegexAction(DBAction):    
    """ Action en Base de Données utilisant une regex """
    def __init__(self,regex,colname,debug=True):
        self.regex   = re.compile(regex)
        self.colname = colname
        DBAction.__init__(self,debug)
    def getQuery(self):         return "SELECT interactionId,talk_clean FROM lines"
    def getQueryUpdate(self):   return "UPDATE lines SET {}_count=(%s), {}=(%s) WHERE interactionid =(%s)".format(self.colname,self.colname)
    
    def loop(self,data):
        _interactionId,_talk = data
        _matches = list(m[0] for m in self.regex.findall(_talk))
        _matchescount  = len(_matches)
        _ncursor = self.cnxn.cursor()
        _matches = '|'.join(_matches)
        _ncursor.execute(self.getQueryUpdate(),(_matchescount,_matches,_interactionId) )
        
class TalkCleanAction(DBAction):    
    def __init__(self,debug=True,month=10):
        self.month = month
        self.file = open(os.path.join("results","raw_talk_clean_{:02d}.txt".format(self.month)),"w",encoding="utf-8")
        DBAction.__init__(self,debug)
    def getQuery(self):         
        return "SELECT interactionId,talk_clean FROM lines WHERE EXTRACT(MONTH FROM file_date) = {:02d} AND talk_empty = false".format(self.month)
    
    def loop(self,data):
        self.file.write('###'+str(data[0])+'###\n')
        self.file.write(data[1]+'\n')
        self.file.write('-'*10+'\n')

    def finalize(self):
        self.file.close()
            
class SearchAction(DBAction):
    """ Action en Base de Données utilisant les modules de ReferenceSearch """
    def __init__(self,module,colname,filename,path,debug=True):
        self.search  = module(filename=filename,path=path)
        self.colname = colname
        DBAction.__init__(self,debug)
        
    def getQuery(self):         return "SELECT interactionId,talk_words FROM lines WHERE talk_wordcount > 0"
    def getQueryUpdate(self):   return "UPDATE lines SET {}_count=(%s), {}=(%s) WHERE interactionid =(%s)".format(self.colname,self.colname)

    def finalize(self):         self.search.finalize()
         
    def updateDB(self,key,value,valueslist):
        _ncursor = self.cnxn.cursor()
        _strlist = '|'.join(valueslist)
        _ncursor.execute(self.getQueryUpdate(),(value,_strlist,key) )
                    
    def loop(self,data):
        _interactionId,_talk = data
        _result,_found = self.search.do(_talk) 
        # s'il y a un résultat, on met à jour la DB
        if _result : 
            self.updateDB(_interactionId,_result,_found)
            
class TreeTaggerSearchAction(SearchAction):
    def getQuery(self):         return "SELECT interactionId,tree_tagger FROM lines WHERE talk_wordcount > 0"
    def loop(self,data):
        _interactionId,_talk = data
        if _talk is None : return
        _lemlist = list(a.split('\t') for a in _talk.split('|') if a is not None)
        _allowed = ['NOM','VER','ADV','ADJ']
        _lemmes  = list()
        for _lem in _lemlist:
            if len(_lem) <3 : break
            for _ok in _allowed :
                if _lem[1].startswith(_ok):
                    _lemmes.append(_lem[2].replace('/','|'))
                    break
        _result,_found = self.search.do('|'.join(_lemmes)) 
        # s'il y a un résultat, on met à jour la DB
        if _result : 
            self.updateDB(_interactionId,_result,_found)
            
class WordOccurencesAction(DBAction):
    def __init__(self,debug=True,file='occurences.csv'):
        self.occurences = dict()
        self.file       = file
        DBAction.__init__(self,debug)
    
    def getQuery(self): return "SELECT interactionId,EXTRACT(MONTH FROM file_date),talk_words FROM lines WHERE talk_empty=false"
    
    def loop(self,data):
        _id,_month,_wordsStr = data
        _words = _wordsStr.lower().split('|')
        for _w in _words : 
            # On ajoute la clé si elle n'existe pas
            if _w not in self.occurences.keys():
                self.occurences[_w] = [0 for _a in range(13)]
            # On met a jour le compteur d'occurences
            self.occurences[_w][0]+=1
            self.occurences[_w][int(_month)]+=1
            
    def finalize(self):
        FILE_OUT = open(self.file,'w',encoding='utf-8-sig',newline='')
        outwriter = csv.writer(FILE_OUT, delimiter=';', quotechar='"',quoting=csv.QUOTE_ALL)
        outwriter.writerow(['mot','total','janvier','fevrier','mars','avril','mai','juin','juillet','aout','septembre','octobre','novembre','decembre',])
        for _k,_v in self.occurences.items():
            _res = [_k,]
            _res.extend(_v)
            outwriter.writerow(_res)
        FILE_OUT.close()
        
class ExtractForLearningAction(DBAction):
    def __init__(self,debug=True,file='output.txt'):
        self.lines = list()
        self.file  = file
        DBAction.__init__(self,debug)
        
    def getQuery(self): return "SELECT talk_clean FROM lines WHERE talk_clean NOT LIKE '%[URL]%' AND talk_clean NOT LIKE '%[ASP]%' AND talk_empty=false LIMIT 10000"
    def loop(self,data): self.lines.append(data[0])
    def finalize(self):
        with open(self.file,'w',encoding='utf-8-sig') as FILE_OUT : 
            for _line in self.lines:
                FILE_OUT.write(_line)
                FILE_OUT.write('\n')
                
            
            
# ---------------------------------------------------------------------------------------------------------------------
#                                   SPECIALISATIONS
# ---------------------------------------------------------------------------------------------------------------------

class MultipleLettersAction(RegexAction):
    def __init__(self,debug=True):
        RegexAction.__init__(self,r'([^\W\d]*([^\W\d])\2{2,}[^\W\d]*)','multiple_letters',debug=debug)

class MultiplePunctAction(RegexAction):
    def __init__(self,debug=True):
        # RegexAction.__init__(self,r'((\W)\2{1,})','multiple_punct',debug=debug)
        RegexAction.__init__(self,r'(([!?;.])\2{1,})','multiple_punct',debug=debug)
        
class LiwcAction(SearchAction):
    def __init__(self,debug=True):
        SearchAction.__init__(self,LiwcSearch,"liwc","raw_results_liwc","References//FrenchLIWCDic_words.txt",debug=debug)
    
class SmileysAction(SearchAction):
    def __init__(self,debug=True):
        SearchAction.__init__(self,SmileySearch,"smileys","raw_results_smileys","References//smileys.txt",debug=debug)
    def getQuery(self): return "SELECT interactionId,talk_clean FROM lines"
        
class ArgotAction(SearchAction):
    def __init__(self,debug=True):
        SearchAction.__init__(self,ReferenceSearch,"argot","raw_results_argot","References//argot.txt",debug=debug)
        
class InterjectionsAction(SearchAction):
    def __init__(self,debug=True):
        SearchAction.__init__(self,ReferenceSearch,"interjections","raw_results_interjections","References//interjections.txt",debug=debug)        

class LiwcTreeTagger(TreeTaggerSearchAction):
    def __init__(self,debug=True):
        TreeTaggerSearchAction.__init__(self,LiwcSearch,"liwc_tree_tagger","raw_results_liwc_TT","References//FrenchLIWCDic_words.txt",debug=debug)
        
# ---------------------------------------------------------------------------------------------------------------------
#                                           MAIN
# ---------------------------------------------------------------------------------------------------------------------        

if __name__ == "__main__":
    DEBUG_MODE = True
    # for _month in range(1,13):    TalkCleanAction(DEBUG_MODE,_month)
    # DuplicateAction(DEBUG_MODE) 
    # ArgotAction(DEBUG_MODE) 
    # InterjectionsAction(DEBUG_MODE) 
    # SmileysAction(DEBUG_MODE) 
    # LiwcAction(DEBUG_MODE) 
    # CountAnswerWordAction(DEBUG_MODE) 
    # MultipleLettersAction(DEBUG_MODE)
    # MultiplePunctAction(DEBUG_MODE)
    # WordOccurencesAction(DEBUG_MODE)
    # LiwcTreeTagger(DEBUG_MODE)
    ExtractForLearningAction(DEBUG_MODE)
