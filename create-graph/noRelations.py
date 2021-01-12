import sqlite3
import networkx as nx
from itertools import combinations
import time

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
def isRelationalTable(fkList, pkList):
    tempFk = []
    tempPk = []
    for fk in fkList:
        for label in fkList[fk]:
            tempFk.append(label[0])
    for pk in pkList:
        tempPk.append(pk)

    for pk in tempPk:
        if pk in tempFk:
            return True
    
    return False

# Change the connect address to whaterver sqlite database you want to access in your local directory
conn = sqlite3.connect("/home/ozan/spider/database/college_2/college_2.sqlite")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

graph = nx.Graph()

# Find all of the tables inside the current database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# List consisting of tuples, the first element of the tuple is the table name
tableList = cursor.fetchall()

rowCounts = {}
foreignKeys = {}
for table in tableList:
    cursor.execute("PRAGMA foreign_key_list({})".format(sql_identifier(table['name'])))
    foreignKeys[table['name']] = None
    foreignKeysInfo = {}

    for row in cursor:
        key = str(row['id']) + "_" + row['table']
        if key not in foreignKeysInfo:
            foreignKeysInfo[key] = []
        foreignKeysInfo[key].append((row['from'],row['to']))
    foreignKeys[table['name']] = foreignKeysInfo

for key in foreignKeys:
    print(key)
    print(foreignKeys[key]) 

# Create the nodes
for table in tableList:
    # Name of the current table, extracted from the tuple
    tableName = table['name']
    print(tableName)
    # First, get the primary key information of this table
    query = "PRAGMA table_info({})".format(tableName)
    cursor.execute(query)
    # ADD COMMENT HERE
    primaryKeyAttributes = []
    for row in cursor:
        keyValue = row['pk']
        if keyValue > 0:
            primaryKeyInfo = (row['cid'],row['name'])
            primaryKeyAttributes.append(primaryKeyInfo)

    #query to retrieve all rows
    query = 'SELECT * FROM {}'.format(tableName)
    #execute the query
    cursor.execute(query)

    #iterate over the rows to make each row a node in the graph
    for row in cursor:
        # Get the primary key values from the primary key list for the row
        primaryKeys = {}
        for keyAttribute in primaryKeyAttributes:
            temp =  keyAttribute[1]
            primaryKeys[temp] = row[keyAttribute[0]]

        isTableRelational = isRelationalTable(foreignKeys[tableName], primaryKeys) #TODO

        if isTableRelational == False:
            # Create an id for the node
            nodeId = createNodeId(tableName, primaryKeys)
            
            nodes = []
            for (p,d) in graph.nodes(data=True):
                if d['node_id'] == nodeId:
                    nodes.append(p)
                    break
            if len(nodes) == 0:
                #Add node to the graph
                
                graph.add_node(nodeId, node_id=nodeId, table_name=tableName, primary_keys=primaryKeys)

            # Check if this table has foreign keys
            if len(foreignKeys[tableName]) != 0:
                for entry in foreignKeys[tableName]:
                    entryValue = entry[2:]
                    valueList = {}
                    for label in foreignKeys[tableName][entry]:
                        valueList[label[1]] = row[label[0]]
                    fkNodeId = createNodeId(entryValue,valueList)
                    nodes = []
                    for (p,d) in graph.nodes(data=True):
                        if d['node_id'] == fkNodeId:
                            nodes.append(p)
                            break
                    if len(nodes) == 0:
                        graph.add_node(fkNodeId, node_id=fkNodeId, table_name=entryValue, primary_keys=valueList)
                        graph.add_edge(nodeId, fkNodeId)
                    else:
                        graph.add_edge(nodeId,nodes[0])

        else:
            if len(foreignKeys[tableName]) != 0:
                fkNodes = []
                info = {}
                for entry in foreignKeys[tableName]:
                    entryValue = entry[2:]
                    valueList = {}
                    for label in foreignKeys[tableName][entry]:
                        valueList[label[1]] = row[label[0]]
                    fkNodeId = createNodeId(entryValue, valueList)
                    info[fkNodeId] = valueList
                    fkNodes.append(fkNodeId)
                # Check if nodes exist, create them if not  
                for fk in fkNodes:
                    nodes = []
                    for (p,d) in graph.nodes(data=True):
                        if d['node_id'] == fk:
                            nodes.append(p)
                            break
                    if len(nodes) == 0:
                        graph.add_node(fk, node_id=fk, table_name = fk.split('_')[0], primary_keys=info[fk])


                for fk1,fk2 in combinations(fkNodes, 2):
                    graph.add_edge(fk1, fk2)
                
nx.write_gml(graph, "no_relations_graph.gml")