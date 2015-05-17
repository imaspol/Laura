#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        CSVtoDB
# Purpose:     Load the corpus contained in CSV into a PostgreSQL DB
#
# Author:      Irina Maslowski
#
# Created:     07/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

import os,sys
import re
import csv
import datetime
import psycopg2

class LineData(object):
    """ Traite les informations relative à une ligne """
        
    def __init__(self,filename,listItem):
        """ Creation d'un LineData a partir d'une liste de str"""
        self.filename = filename
        self.keys = ['date','id','dialogId','interactionId','civ','talk','answer'] #liste d'entetes de colonnes
        # Erreur si le nombre de colonne ne correspond pas au nombre de cles
        if len(listItem) != len(self.keys) :
            print(listItem)
            print(len(listItem),len(self.keys))
            raise Exception("Nombre de colones incorrect")

        # Pour chaque cle, on cree dynamiquement l'attribut correspondant avec la valeur correspondante
        for _i,_k in enumerate(self.keys):
            setattr(self,_k,listItem[_i])
            
    def _clean(self,value):
        # Regex pour éliminer les URL
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
        _newline = re.sub(re_href,"\t[URL]\t",value)
        _newline = re.sub(re_tags,"\t[URL]\t",_newline)
        _newline = re.sub(re_url,"\t[URL]\t",_newline)
        _newline = re.sub(re_asp,"\t[ASP]\t",_newline)
        _newline = re.sub(re_mail,"\t[MAIL]\t",_newline)
        _newline = re.sub(re_date1,"\t[DATE]\t",_newline)
        _newline = re.sub(re_date2,"\t[DATE]\t",_newline)
        _newline = re.sub(re_date3,"\t[DATE]\t",_newline)
        return _newline
        
    def do_clean(self):
        """ Netoyage du text utilisateur"""
        self.talk_clean   = self._clean(self.talk)
        self.answer_clean = self._clean(self.answer)
        
    def do_tokenize(self):
        """ Applique les tokenizers sur les donnees"""
        # self.sentences = nltk.sent_tokenize(self.talk_clean)
        # On ajoute la nouvelle colonne a la liste des cles (qui va servir à créer une colonne suplémentaire en sortie)
        self.talk_words = re.split('\W+', self.talk_clean, flags = re.UNICODE)
        self.talk_words = list(filter(('').__ne__,self.talk_words))
        self.talk_wordcount = len(self.talk_words)
        self.answer_words = re.split('\W+', self.answer_clean, flags = re.UNICODE)
        self.answer_words = list(filter(('').__ne__,self.answer_words))
        self.answer_wordcount = len(self.answer_words)
        if self.answer_clean.find('\t[URL]\t') > -1 or self.answer_clean.find('\t[ASP]\t') > -1 : 
            self.answer_url = 1 
        else : 
            self.answer_url = 0 

    def do_dbAdd(self,cnxn):
        """ Sauvegarde les données en base"""
        cur  = cnxn.cursor()
        query = """INSERT INTO lines 
                    (file,date ,id ,dialogId ,interactionId ,civ , 
                    talk,talk_clean ,talk_words ,talk_wordcount ,
                    answer ,answer_clean ,answer_words ,answer_wordcount)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                    
        _values = [self.filename, self.date, self.id ,int(self.dialogId), int(self.interactionId), self.civ, 
                    self.talk, self.talk_clean, "|".join(self.talk_words), int(self.talk_wordcount),
                    self.answer, self.answer_clean,"|".join(self.answer_words), int(self.answer_wordcount),]
        cur.execute(query, _values)
        cnxn.commit()
        
        
class FileData(object):
    """ Assure la gestion d'une liste de LineData, correspondant a chaque ligne du fichier"""
    
    def __init__(self,dirPath,separator = '|'):
        """ Charge le fichier dirPath et cree les LineData a partir des lignes """
        self.lines = list()
        self.separator = separator
        self.idMap = dict() 
        self.file = os.path.basename(dirPath)
        for _fileName in os.listdir(dirPath):
            # Ouverture du fichier
            # L'encodage "utf-8-sig" correspond a UTF avec BOM
            _filePath = os.path.join(dirPath,_fileName)
            with open(_filePath, newline='', encoding="utf-8-sig") as _csvfile : 
                # Parcours du fichier
                _csvreader = csv.reader(_csvfile, delimiter='|')
                for _n,_line in enumerate(_csvreader) :
                    # On ajoute toutes les lignes sauf la premiere
                    if _n == 0 : pass
                    else : self.append(_fileName,_line)
            self.idMap.clear()
            print (_filePath,_n,'\n\tLignes parcourues')
        print('-----------------------------------------------------------')
    
    def append(self,_fileName,line):
        """ Cree un LineData a partir de la ligne et l'ajoute a la liste"""
        # On cree le LineData a partir des elements de la ligne
        try : 
            _data=LineData(_fileName,line)
            self.lines.append(_data)   
        # En cas d'erreur on affiche l'erreur
        except Exception as err:            
            print(err)
            raise
        # Affichage du compte
        return True
                
    def do_loop(self):
        """ Parcours des lignes et application des opérations """
        # ---------- INITIALISATION DES ACTIONS ----------------
        # en fonction de résultats qu'on a besoin, on peut commenter ou descommenter
        cnxn = psycopg2.connect(database='laura_test', user='postgres', password='admin', port="5433") 
        # ---------------- PARCOURS DES LIGNES --------------------
        for _n,_line in enumerate(self.lines) :
            # Nettoyage du texte utilisateur
            _line.do_clean()
            # Appel du Tokenizer
            _line.do_tokenize()
            _line.do_dbAdd(cnxn)
            if _n % 100 == 0: print('{:.2f}%'.format(_n/float(len(self.lines))), end='\r')
                    
        # ------------- FINALISATION DES ACTIONS ----------------
        cnxn.close()

if __name__ == "__main__":    
    """
    RESET_DB = False
    if RESET_DB : 
        cnxn = psycopg2.connect(database='laura_test', user='postgres', password='admin', port="5433") 
        cur  = cnxn.cursor()
        cur.execute('''DROP TABLE IF EXISTS lines;
                       CREATE TABLE lines(file text, date text, id text ,dialogId bigint, interactionId bigint PRIMARY KEY, civ text, 
                        talk text, talk_clean text, talk_words text, talk_wordcount integer, talk_empty boolean, duplicate bigint,
                        answer text, answer_clean text,answer_words text, answer_wordcount integer, answer_empty boolean, url interger)''')
        cur.execute('''ALTER TABLE lines ADD COLUMN smileys TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN smileys_count INT NOT NULL DEFAULT 0''')     
        cur.execute('''ALTER TABLE lines ADD COLUMN liwc TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN liwc_count INT NOT NULL DEFAULT 0''')         
        cur.execute('''ALTER TABLE lines ADD COLUMN argot TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN argot_count INT NOT NULL DEFAULT 0''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN interjections TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN interjections_count INT NOT NULL DEFAULT 0''')
        cur.execute('''ALTER TABLE lines ADD COLUMN multiple_punct TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN multiple_punct_count INT NOT NULL DEFAULT 0''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN multiple_letters TEXT''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN multiple_letters_count INT NOT NULL DEFAULT 0''')                        
        cur.execute('''ALTER TABLE lines ADD COLUMN stamp TIMESTAMP WITH TIME ZONE''')
        cur.execute('''UPDATE lines SET stamp=to_timestamp(date,'YYYY-MM-DD HH24:MI:SS.0')''')
        cur.execute('''ALTER TABLE lines ADD COLUMN satisfaction TEXT''')
        cnxn.commit()
        cnxn.close()
            
    myDir = ".\\CSVciv\\10"
    data = FileData(myDir)
    data.do_loop()
    """ 
    
    TIMENOW        = lambda : datetime.datetime.now().time().strftime('%H:%M:%S')
    _dir = 'D:\\satisfaction\\'
    cnxn = psycopg2.connect(database='laura', user='postgres', password='admin', port="5433") 
    cur  = cnxn.cursor()
    print(TIMENOW(),'CNXN OK','-'*30)
    for _filename in os.listdir(_dir):
        print(TIMENOW(),'FILE',_filename,'-'*15)
        # Ouverture du fichier
        # L'encodage "utf-8-sig" correspond a UTF avec BOM
        _filePath = os.path.join(_dir,_filename)
        with open(_filePath, newline='', encoding="utf-8-sig") as _csvfile : 
            # Parcours du fichier
            _csvreader = csv.reader(_csvfile, delimiter='|')
            for _n,_line in enumerate(_csvreader) :
                # On ajoute toutes les lignes sauf la premiere
                if _n == 0 : pass
                elif len(_line[2])==0 : pass
                else : 
                    cur.execute('''UPDATE lines SET satisfaction=(%s) WHERE interactionid =(%s)''',[_line[2],_line[1],])
            cnxn.commit()
    print(TIMENOW(),'Finalize','-'*30)
    cnxn.close()    
    print('\a'*5) #Beep
