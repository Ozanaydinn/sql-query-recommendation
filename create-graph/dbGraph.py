import sqlite3
import networkx as nx
import time

def sql_identifier(s):
    return '"' + s.replace('"', '""') + '"'

# Change the connect address to whatever sqlite database you want to access in your local directory
conn = sqlite3.connect("/home/ozan/spider/database/college_2/college_2.sqlite")
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

graph = nx.Graph()

"""Creates a unique node id in the form of "tablename_primarykey1_primarykey2_....
This node ID will be used during the search of a node in the graph.
"""
def createNodeId(tableName, primaryKeys):
    nodeId = tableName
    for key in primaryKeys:
        nodeId = nodeId + "_" + str(primaryKeys[key])
    return nodeId


# Find all of the tables inside the current database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# List consisting of tuples, the first element of the tuple is the table name
tableList = cursor.fetchall()

rowCounts = {}
foreignKeys = {}
for table in tableList:
    rowCounts[table['name']] = 0
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

time.sleep(1)

# Create the nodes
for table in tableList:
    # Name of the current table, extracted from the tuple
    tableName = table['name'] #table['name']
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
                print("Printing entry : {}".format(entry))
                entryValue = entry[2:]
                valueList = {}
                for label in foreignKeys[tableName][entry]:
                    valueList[label[1]] = row[label[0]]
                print("Printing valuelist  : {}.".format(valueList))
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
nx.write_gml(graph, "graph.gml")