import sqlite3
import networkx as nx
from itertools import combinations
import time
from hashedindex import textparser
import hashedindex

def sql_identifier(s):
    return '"' + s.replace('"', '""') + '"'
    
"""Creates a unique node id in the form of "tablename_primarykey1_primarykey2_....
This node ID will be used during the search of a node in the graph.
"""
def createNodeId(tableName, primaryKeys):
    nodeId = tableName
    for key in primaryKeys:
        nodeId = nodeId + "_" + str(primaryKeys[key])
    return nodeId


"""Checks if a table is a relational table by looking at their foreign keys and primary keys. If they are exactly the same, returns True, otherwise return False.
Args:
    fkList (list): List of foreign keys of the table
    pkList (list): List of primary keys of the table
"""
def isRelationalTable(fkList, pkList, column_count):
    tempFk = []
    tempPk = []
    for fk in fkList:
        for label in fkList[fk]:
            tempFk.append(label[0])
    for pk in pkList:
        tempPk.append(pk[1])

    if (column_count - len(tempPk)) == len(tempFk):
        return True

    for pk in tempPk:
        if pk not in tempFk:
            return False
    
    return True

# Change the connect address to whatever sqlite database you want to access in your local directory
conn = sqlite3.connect("C:\\Users\\ozana\\Downloads\\advising-db.added-in-2020.sqlite")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

graph = nx.Graph()

# Find all of the tables inside the current database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# List consisting of tuples, the first element of the tuple is the table name
tableList = cursor.fetchall()

nodeIds = {}
rowCounts = {}
foreignKeys = {}
documents = []
index = hashedindex.HashedIndex()
for table in tableList:
    cursor.execute("PRAGMA foreign_key_list({})".format(sql_identifier(table['name'])))
    foreignKeys[table['name'].lower()] = None
    foreignKeysInfo = {}

    for row in cursor:
        key = (str(row['id']) + "_" + row['table']).lower()
        if key not in foreignKeysInfo:
            foreignKeysInfo[key] = []
        foreignKeysInfo[key].append((row['from'].lower(),row['to'].lower()))
    foreignKeys[table['name'].lower()] = foreignKeysInfo

# This is done on Büşra's request, on input, just press enter if you are not Büşra :)
relations = input("Please enter a comma seperated list of tables: ") 
relations = relations.split(',')
idx = 0
rows = {}
print(relations)
# Create the nodes
for table in tableList:
    # Name of the current table, extracted from the tuple
    tableName = table['name'].lower()  #"offering_instructor"
    # First, get the primary key information of this table
    query = "PRAGMA table_info({})".format(tableName)
    cursor.execute(query)
    #column_count = len(cursor)
    # Get the primary key information from the PRAGMA command
    primaryKeyAttributes = []
    for row in cursor:
        keyValue = row['pk']
        if keyValue > 0:
            primaryKeyInfo = (row['cid'],row['name'])
            primaryKeyAttributes.append(primaryKeyInfo)
    print(primaryKeyAttributes)
    #query to retrieve all rows
    query = 'SELECT * FROM {}'.format(tableName)
    #execute the query
    cursor.execute(query)
    column_count = 0
    foreignList = []
    # Get the foreign key info into a list
    if len(foreignKeys[tableName]) != 0:
        for entry in foreignKeys[tableName]:
            for val in foreignKeys[tableName][entry]:
                foreignList.append(val[0])

    # find the column number
    for row in cursor:
        column_count = len(row.keys())
        break

    isTableRelational = isRelationalTable(foreignKeys[tableName], primaryKeyAttributes, column_count) #TODO
    if tableName == "offering_instructor":
        # Find the column with max relations
        count_dict = {}
        max_relation_column = ""
        if isTableRelational == True:
            max_relation_count = -1
            for row in cursor:
                for key in row.keys():
                    print(key)
                    max_relation_query = 'select count(*) as result from {} group by {} order by result desc limit 1'.format(tableName, key)
                    cursor.execute(max_relation_query)
                    for value in cursor:
                        if max_relation_count < value['result']:
                            max_relation_count = value['result']
                            max_relation_column = key
                break
            print("max", max_relation_column)

        # find all the counts from our max. relation column
        count_query = 'select {}, count(*) as result from {} group by {}'.format(max_relation_column, tableName, max_relation_column)
        cursor.execute(count_query)
        for row in cursor:
            count_dict[row[max_relation_column]] = row['result']
        print(count_dict)
    query = 'SELECT * FROM {}'.format(tableName)
    #execute the query
    cursor.execute(query)
    #iterate over the rows to make each row a node in the graph
    for row in cursor:
        doc = []
        # Get the primary key values from the primary key list for the row
        primaryKeys = {}
        for keyAttribute in primaryKeyAttributes:
            temp =  keyAttribute[1]
            primaryKeys[temp] = row[keyAttribute[0]]


        if isTableRelational == False:
            # Create an id for the node
            nodeId = createNodeId(tableName, primaryKeys)
            # Get all the values from row
            for value in row:
                isForeign = False
                # Check if a word is foreign
                for entry in foreignList:
                    if row[entry] == value:
                        isForeign = True
                if isForeign == False:
                    doc.append(str(value)) # add words to the row documentS

            str_doc =  ' '.join(str(e) for e in doc)
            str_doc = str_doc.lower()
            documents.append(str_doc)
            rows[idx] = nodeId
            idx+=1
            
            if nodeId not in nodeIds:
                nodeIds[nodeId] = ""
                graph.add_node(nodeId, node_id=nodeId, table_name=tableName, primary_keys=primaryKeys,type="doc")
            """
            nodes = []
            for (p,d) in graph.nodes(data=True):
                if d['node_id'] == nodeId:
                    nodes.append(p)
                    break
            if len(nodes) == 0:
                #Add node to the graph
                graph.add_node(nodeId, node_id=nodeId, table_name=tableName, primary_keys=primaryKeys,type="doc")
            """

            # Check if this table has foreign keys
            if len(foreignKeys[tableName]) != 0:
                for entry in foreignKeys[tableName]:
                    entryValue = entry[2:]
                    valueList = {}
                    if entryValue not in relations and relations[0] != '':
                        continue
                    for label in foreignKeys[tableName][entry]:
                        valueList[label[1]] = row[label[0]]
                    
                    # Check if valueList contains None as a value, if it does, do not create a node for it.
                    containsNone = False
                    for key in valueList:
                        if valueList[key] == None:
                            containsNone = True
                    if containsNone == False:
                        fkNodeId = createNodeId(entryValue,valueList)
                        if fkNodeId not in nodeIds:
                            nodeIds[fkNodeId] = ""
                            graph.add_node(fkNodeId, node_id=fkNodeId, table_name=entryValue, primary_keys=valueList,type="doc")
                            graph.add_edge(nodeId, fkNodeId, weight=1)
                        else:
                            graph.add_edge(nodeId,fkNodeId,weight=1)
                        """
                        nodes = []
                        for (p,d) in graph.nodes(data=True):
                            if d['node_id'] == fkNodeId:
                                nodes.append(p)
                                break
                        if len(nodes) == 0:
                            graph.add_node(fkNodeId, node_id=fkNodeId, table_name=entryValue, primary_keys=valueList,type="doc")
                            graph.add_edge(nodeId, fkNodeId, weight=1)
                        else:
                            graph.add_edge(nodeId,nodes[0],weight=1)
                        """

        else:
            if tableName == "offering_instructor":
                if count_dict[row[max_relation_column]] > 10:
                    if len(foreignKeys[tableName]) != 0:
                        fkNodes = []
                        info = {}
                        for entry in foreignKeys[tableName]:
                            entryValue = entry[2:]
                            if entryValue not in relations and relations[0] != '':
                                continue
                            valueList = {}
                            for label in foreignKeys[tableName][entry]:
                                valueList[label[1]] = row[label[0]]
                            fkNodeId = createNodeId(entryValue, valueList)
                            info[fkNodeId] = valueList
                            fkNodes.append(fkNodeId)
                        # Check if nodes exist, create them if not  
                        for fk in fkNodes:
                            if fk not in nodeIds:
                                nodeIds[fk] = ""
                                graph.add_node(fk, node_id=fk, table_name = fk.split('_')[0], primary_keys=info[fk], type="doc")
                            """
                            nodes = []
                            for (p,d) in graph.nodes(data=True):
                                if d['node_id'] == fk:
                                    nodes.append(p)
                                    break
                            if len(nodes) == 0:
                                graph.add_node(fk, node_id=fk, table_name = fk.split('_')[0], primary_keys=info[fk], type="doc")
                            """

                        for fk1,fk2 in combinations(fkNodes, 2):
                            graph.add_edge(fk1, fk2,weight=1)
            else:
                if len(foreignKeys[tableName]) != 0:
                    fkNodes = []
                    info = {}
                    for entry in foreignKeys[tableName]:
                        entryValue = entry[2:]
                        if entryValue not in relations and relations[0] != '':
                            continue
                        valueList = {}
                        for label in foreignKeys[tableName][entry]:
                            valueList[label[1]] = row[label[0]]
                        fkNodeId = createNodeId(entryValue, valueList)
                        info[fkNodeId] = valueList
                        fkNodes.append(fkNodeId)
                    # Check if nodes exist, create them if not  
                    for fk in fkNodes:
                        if fk not in nodeIds:
                                nodeIds[fk] = ""
                                graph.add_node(fk, node_id=fk, table_name = fk.split('_')[0], primary_keys=info[fk], type="doc")
                        """
                        nodes = []
                        for (p,d) in graph.nodes(data=True):
                            if d['node_id'] == fk:
                                nodes.append(p)
                                break
                        if len(nodes) == 0:
                            graph.add_node(fk, node_id=fk, table_name = fk.split('_')[0], primary_keys=info[fk], type="doc")
                        """

                    for fk1,fk2 in combinations(fkNodes, 2):
                        graph.add_edge(fk1, fk2,weight=1)

for doc in documents:
    for term in textparser.word_tokenize(doc, ignore_numeric=False):
        index.add_term_occurrence(term, doc)
print(len(index.terms()))

for term in index.terms():
    # Create node for term
    termNodeId = ''.join(term)
    graph.add_node(termNodeId, node_id=termNodeId, table_name="", primary_keys={},type="word")

idx = 0
for doc in documents:
    # Get the node id for the document node
    docNodeId = rows[idx]
    idx+=1
    for term in textparser.word_tokenize(doc, ignore_numeric=False):
        tfidf = index.get_tfidf(term, doc, normalized=True)
        # Add edge using tfidf data
        graph.add_edge(docNodeId, ''.join(term), weight=tfidf)

nx.write_gml(graph, "no_relations_graph_gcn_adv.gml") 