# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 01:08:22 2021

@author: 90506
"""
import numpy as np
import sqlite3
import pandas as pd
from gensim.models import Word2Vec
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

corpus=[]
for df in  dataframe_list:
    df2= df.apply(lambda x: ','.join(x.astype(str)),axis=1)
    df_clean= pd.DataFrame({'clean':df2})
    sent = [row.split(',') for row in df_clean['clean']]
    for i in sent:
        corpus.append(i)
model = Word2Vec(corpus, min_count=1,size= 50,workers=3, window =3, sg = 1)   
list_res=[]
id_count = 0
prime_tables = ['time_slot','student','section','instructor','course','department','classroom']
counter = 0
for df in  dataframe_list:
    if names[counter] in prime_tables:
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
#print(df)  
df.to_csv('output_w2v.csv')
#w = csv.writer(open("output.csv", "w"))
#for key, val in dict_res.items():
 #   w.writerow([key, val])