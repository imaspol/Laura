#!/usr/bin/python
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        reference_search.py
# Purpose:     specialized classes for word searching in reference dictionaries
#
# Author:      Irina Maslowski
#
# Created:     07/04/2015
# Copyright:   (c) Irina Maslowski 2015
#-------------------------------------------------------------------------------

import os,re

class ReferenceSearch(object):
    def __init__(self,filename,path):
        """ Initialise le dictionnaire de mots et création du fichier de sortie"""
        # Sauvegarde du prefixe de fichiers
        self.filename=filename
        # Création du dico de références
        self.ref_dict = dict()
        # Ouverture du fichier
        with open(path,encoding="utf-8") as _file : 
            for _n,_line in enumerate(_file) : 
                _data = _line.rstrip().split("\t")
                # On ajoute la ligne courante dans de dico de références
                _value = _data[0].lower() # Mot
                _def = " ".join(_data[1:]) # Definition
                if _value not in self.ref_dict.keys() and len(_value):
                    self.ref_dict[_value]=_def
        # Création du dictionnaire de résultats
        self.results = dict.fromkeys(self.ref_dict.keys(),0)
        
    def findall(self,line,value):
        """ La fonction renvoie la liste des positions du mot"""
        resultat=[]
        precedent=0
        index=line.find(value)
        while index!=-1:
            resultat.append(precedent+index)
            precedent=precedent+index+len(value)
            index=line[precedent:].find(value)
        return resultat
        
    def do(self,line):
        # pour chaque mot  du dico, on cherche si il est dans la ligne
        _cpt = 0 
        _found = list()
        _line = line.lower().split('|')
        for _arg in self.results.keys() : 
            _pos = _line.count(_arg)
            if _pos:
                #Le mot est dans la ligne
                self.results[_arg] += _pos
                _cpt += _pos
                _found.append(_arg)
        return _cpt,_found
        
    def finalize(self):
        """ Création du fichier de résultat """
        # Création du fichier de mots
        _filePath = "Results//{}.txt".format(self.filename)
        with open(_filePath,"w", encoding="utf-8") as file: 
            for _k,_c in self.results.items() : 
                if _c > 0 :
                    _output = "{}\t{}\t{}\n".format(_k,_c,self.ref_dict[_k])
                    file.write(_output)

        # Test d'existence
        return os.path.exists(_filePath)
        
class SmileySearch(ReferenceSearch):
            
    def do(self,line):
        _cpt = 0 
        _found = list()
        # pour chaque smiley du dico, on cherche si il est dans la ligne
        for _sm in self.results.keys() : 
            _pos = self.findall(line,_sm)
            if len(_pos):
                #Le smiley est dans la ligne
                self.results[_sm] += len(_pos)
                _cpt += len(_pos)
                _found.append(_sm)
        return _cpt,_found
        
REFERENCE = {   '19':"négation",
                '22':"juron",
                '125':"affect",
                '126':"émopos",
                '127':"émonég",
                '128':"anxiété",
                '129':"colère",
                '130':"tristesse",
                '134':"divergence",
                '149':"sexualité",
                '355':"accomplissement",
                '462':"consentement",
                '463':"hésitation",
                '464':"remplisseur",}

class LexicTreeElement(object):
    def __init__(self):
        self.children = dict()
        self.values   = None
        self.key      = None
        
    def add(self,word,key,*values):
        # On est arrivé au bout du mot, on stocke les valeurs
        if not len(word):
            self.values = values
            self.key    = key
        # Il reste des lettres
        else : 
            # Pas d'enfant pour la lettre suivante, on le cree
            if word[0] not in self.children.keys() :
                self.children[word[0]] = LexicTreeElement()
            # On s'appelle recursivement sur l'enfant
            self.children[word[0]].add(word[1:],key,*values)
            
    def test(self,word):
        if '*' in self.children.keys() : return self.children['*'].key,self.children['*'].values
        if not len(word): return self.key,self.values
        if word[0] not in self.children.keys() : return None,None
        return self.children[word[0]].test(word[1:])
        
class LiwcSearch(ReferenceSearch):
    def __init__(self,filename,path="References//FrenchLIWCDic_words.txt"):
        """ Initialise l'arbre contenant le dico liwc et le dictionnaire de résultats """
        # Sauvegarde du prefixe de fichiers
        self.filename=filename
        # Création de l'élément racine
        self.tree = LexicTreeElement()
        # Création du dictionnaire de référence
        self.ref_dict = dict()
        # Création du dictionnaire de résultats
        self.results = dict()
        # Ouverture du dictionnaire
        with open(path,encoding="utf-8-sig") as _file : 
            for _n,_line in enumerate(_file) : 
                _data = _line.rstrip().split("\t")
                 # On teste si le mot appartient aux champs lexicaux choisis
                if len(set(_data) & set(REFERENCE.keys())) :
                    # On ajoute le mot courant dans l'arbre
                    self.tree.add(_data[0],_data[0],*_data[1:])
                    # On ajoute le mot courant au dictionnaire de référence
                    self.ref_dict[_data[0]]=_data[1:]
        
    def do(self,line):
        """ Test si les mots de line est dans l'arbre liwc"""
        _cpt = 0 
        _found = list()
        _line = line.lower().split('|')
        # On normalise chaque mot en minuscule
        for _word in _line :
            # On teste le mot
            _value,_res = self.tree.test(_word)
            # Si le test est positif (_res != None)
            if _res is not None :
                # Si le mot n'est pas dans le dico de résultats, on l'ajoute
                if _value not in self.results.keys(): self.results[_value]=1
                # Sinon on incrémente le compteur d'occurences
                else : self.results[_value]+=1
                _cpt += 1
                _found.append(_value)
        return _cpt,_found
                    
