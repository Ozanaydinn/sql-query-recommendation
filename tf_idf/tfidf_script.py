# -*- coding: utf-8 -*-
"""
Created on Thu Jan 28 16:19:32 2021

@author: 90506
"""
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
import gensim.downloader as api
from sklearn.feature_extraction.text import TfidfVectorizer

import numpy as np
import sqlite3
import pandas as pd
from gensim.models import Word2Vec
def sql_identifier(s):
    return '"' + s.replace('"', '""') + '"'
def primary_key_lists(listOfImportantTables):
    pKeys = {}
    for name in listOfImportantTables:
        rows = conn.execute("PRAGMA table_info({})".format(sql_identifier(name)))
        listOfKeys =[]
        for row in rows:
            listOfKeys.append(row['name'])
        pKeys[name] = listOfKeys
    return pKeys
def foreign_key_lists(listOfImportantTables):
    fKeys = {}
    for name in listOfImportantTables:
        rows = cursor.execute("PRAGMA foreign_key_list({})".format(sql_identifier(name)))
        listOfKeys =[]
        for row in rows:  
            listOfKeys.append(row['from'])
        fKeys[name] = listOfKeys
    return fKeys

# Change the connect address to whaterver sqlite database you want to access in your local directory
conn = sqlite3.connect("./college_2.sqlite")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
# Find all of the tables inside the current database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# List consisting of tuples, the first element of the tuple is the table name
tableList = cursor.fetchall()
dataframe_list = []
names = []

for table in tableList:
   names.append(table[0])
   df = pd.read_sql_query("SELECT * FROM " + table[0] +";",conn)
   dataframe_list.append(df)

prime_tables = ['time_slot','student','section','instructor','course','department','classroom']   
foreignKeyDict = foreign_key_lists(prime_tables)
primaryKeyDict = primary_key_lists(prime_tables)


corpus=[]
for i in range(0, len(dataframe_list)):
    df = dataframe_list[i]
    #if names[i] in prime_tables:
     #   fkListOfTable = foreignKeyDict[names[i]]
     #   pkListOfTable = primaryKeyDict[names[i]]    
     #   keysToDrop=fkListOfTable #+ pkListOfTable
     #   keysToDrop = set(keysToDrop)
     #   for e in keysToDrop:
     #       df.drop(e,inplace=True,axis =1)
    df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
    df_clean= pd.DataFrame({'clean':df2})
    sent = [str(row).split(',') for row in df_clean['clean']]
    for i2 in sent:
        corpus.append(i2)        
dct = Dictionary(corpus)

counter =0
corpus=[]
holdTheInd ={}
checker = 0
for i in range(0, len(dataframe_list)):
    df = dataframe_list[i]
    if names[i] in prime_tables:
        #print("------")
        #print(counter)
        holdTheInd[names[i]] = [*range(counter, counter + len(df),1)]
        fkListOfTable = foreignKeyDict[names[i]]
        pkListOfTable = primaryKeyDict[names[i]]    
        keysToDrop=fkListOfTable #+ pkListOfTable
        keysToDrop = set(keysToDrop)
        for e in keysToDrop:
            df.drop(e,inplace=True,axis =1)
    df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
    df_clean= pd.DataFrame({'clean':df2})
    sent = [str(row).split(',') for row in df_clean['clean']]
    counter+=1
    for i2 in sent:
        
        corpus.append(dct.doc2bow(i2))  

print(len(corpus))
model = TfidfModel(corpus)  # fit model
vector = model[corpus[0]]
print(counter)
#print(holdTheInd)
prime_tables = ['classroom','course','department','instructor','section','student','time_slot']   
counter = 0
vecs = {}
listForVectors = []
for i in prime_tables:
    
    vector = holdTheInd[i]
    for n in vector:
        counter+=1
        vecForEach = np.zeros(len(corpus)) 
        for e in model[corpus[n]]:
            vecForEach[e[0]] =e[1]    
  
        listForVectors.append(vecForEach.tolist())
 

print(len(listForVectors))
#print(listForVectors)
index = [*range(0,2420,1)]
list_of_tuples = list(zip(index, listForVectors))
df = pd.DataFrame(list_of_tuples, 
                  columns = ['index', 'value'])   
print(df) 
df.to_csv('output_tfidf.csv')