# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 03:26:03 2021

@author: 90506
"""

import sqlite3
import pandas as pd
from gensim.models import Word2Vec
import gensim
def tagged_document(list_of_list_of_words):   
    for i, list_of_words in enumerate(list_of_list_of_words):
      yield gensim.models.doc2vec.TaggedDocument(list_of_words, [i])
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
#corpus=[]
#for df in  dataframe_list:
#    df2= df.apply(lambda x: ' '.join(x.astype(str)),axis=1)
#    df_clean= pd.DataFrame({'clean':df2})
#    sent = [row.split(' ') for row in df_clean['clean']]
#    for i in sent:
#        corpus.append(i)

corpus=[]
for i in range(0, len(dataframe_list)):
    df = dataframe_list[i]
    if names[i] in prime_tables:
        fkListOfTable = foreignKeyDict[names[i]]
        pkListOfTable = primaryKeyDict[names[i]]    
        keysToDrop=fkListOfTable #+ pkListOfTable
        keysToDrop = set(keysToDrop)
        for e in keysToDrop:
           df.drop(e,inplace=True,axis =1)
    df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
    df_clean= pd.DataFrame({'clean':df2})
    sent = [str(row).split(',') for row in df_clean['clean']]
    for i2 in sent:
        corpus.append(i2)  
data_for_training = list(tagged_document(corpus) )

#print(data_for_training [:1])

model = gensim.models.doc2vec.Doc2Vec(vector_size=40, min_count=2, epochs=30)
model.build_vocab(data_for_training)
model.train(data_for_training, total_examples=model.corpus_count, epochs=model.epochs)

#print(model.infer_vector(['classroom', 'Lamberton', '134']))

#prime_tables = ['time_slot','student','section','instructor','course','department','classroom']
counter = 0
required_lists = []
for df in  dataframe_list:
    if names[counter] in prime_tables:
        clmn = list(df)
        for i in range(0,len(df)):
            aList =[]
            for j in clmn:
                aList.append(str(df[j][i]))
            required_lists.append(aList)
    counter +=1 
#take their score
infered_vectors =[]
for i in required_lists:
#    print(i)
    infered_vectors.append(model.infer_vector(i))
print("infered vectors")
#print(infered_vectors)    
index = [*range(0,len(infered_vectors),1)]
list_of_tuples = list(zip(index, infered_vectors))
df = pd.DataFrame(list_of_tuples, 
                  columns = ['index', 'value'])       
df.to_csv('output_doc2vec.csv')