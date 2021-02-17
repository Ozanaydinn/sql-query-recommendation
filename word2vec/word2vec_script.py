# w2v
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 01:08:22 2021

@author: 90506
"""
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
        #print("PRAGMA table_info({})".format(sql_identifier(name)))
        #rows = cursor.execute("PRAGMA foreign_key_list({})".format(sql_identifier(name)))
        listOfKeys =[]
        for row in rows:
            listOfKeys.append(row['name'])
        pKeys[name] = listOfKeys
    return pKeys
def foreign_key_lists(listOfImportantTables):
    fKeys = {}
    for name in listOfImportantTables:
        #print("PRAGMA table_info({})".format(sql_identifier(name)))
        rows = cursor.execute("PRAGMA foreign_key_list({})".format(sql_identifier(name)))
        listOfKeys =[]
        for row in rows:
            #print(row.keys())
            #print(row['from'])   
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
model = Word2Vec(corpus, min_count=1,size= 50,workers=3, window =3, sg = 1)   
list_res=[]
id_count = 0
counter = 0
for df in  dataframe_list:
    if names[counter] in prime_tables:
        fkListOfTable = foreignKeyDict[names[counter]]
        pkListOfTable = primaryKeyDict[names[counter]]    
        keysToDrop=fkListOfTable #+ pkListOfTable
        keysToDrop = set(keysToDrop)
        for e in keysToDrop:
            df.drop(e,inplace=False,axis =1)
        df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
        df_clean= pd.DataFrame({'clean':df2})
        for row in df_clean['clean']:    
            splits = row.split(',')
            empty_list = [0.0 for i in range(0,50)]
           # print(len(empty_list))
            hold = np.array(empty_list)
            noOfCount = 0
            for split in splits:
                #print(len(hold))
                hold = np.add(model[split],hold)
                noOfCount+=1
            hold = np.divide(hold,noOfCount)
            list_res.append(hold.tolist())
            id_count+=1
    counter+=1
#print(list_res)
index = [*range(0,id_count,1)]
list_of_tuples = list(zip(index, list_res))
df = pd.DataFrame(list_of_tuples, 
                  columns = ['index', 'value'])
df.to_csv('output_word2vec.csv')
