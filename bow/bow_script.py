# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 00:52:59 2021

@author: 90506
"""
import numpy as np
import sqlite3
import pandas as pd
from gensim.models import Word2Vec
from sklearn.feature_extraction.text import CountVectorizer

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

holdTheInd ={}
counter =0
corpus=[]
for i in range(0, len(dataframe_list)):
    df = dataframe_list[i]
    if names[i] in prime_tables:
        holdTheInd[names[i]] = [*range(counter, counter + len(df),1)]
        count = 0
        fkListOfTable = foreignKeyDict[names[i]]
        pkListOfTable = primaryKeyDict[names[i]]    
        keysToDrop=fkListOfTable #+ pkListOfTable
        keysToDrop = set(keysToDrop)
        for e in keysToDrop:
            df.drop(e,inplace=True,axis =1)
            
        df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
        df_clean= pd.DataFrame({'clean':df2})
        strings=""
        for row in df_clean['clean']:
            counter+=1
            strings = row.replace(","," ")      
            corpus.append(strings) 
print(counter)
vectorizer = CountVectorizer( binary=True)
vectorizer.fit_transform(corpus)
vectorized_data = vectorizer.transform(corpus)

matrixes =vectorized_data.todense()
counter = 0
listForVectors = []
for i in prime_tables:
    vector = holdTheInd[i]
    for n in vector:
       # print(type(matrixes[n]))
        listForVectors.append(np.squeeze(np.asarray((matrixes[n]))).tolist()) 

#print(listForVectors)
index = [*range(0,2420,1)]
list_of_tuples = list(zip(index, listForVectors))
df = pd.DataFrame(list_of_tuples, 
                  columns = ['index', 'value'])   
print(df) 
df.to_csv('output_bow.csv')
