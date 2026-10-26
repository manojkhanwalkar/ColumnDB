# **Simple column database**
**Features**
1. Column database enables extremely fast queries

**Assumptions**
1. Fixed number of servers in the cluster determined in the beginning 
2. Servers cannot be added or deleted 
3. Data once uploaded is used for querying
4. New databases , tables can be added or deleted. Columns can only be deleted.
5. New data can be added to existing tables, but data cannot be deleted or modified.
6. The max length of a value in a column needs to be specified upfront in the metadata.

The entire cluster can be reset by deleting the underlying files. 

Two examples are provided here - A person with various attributes and a marketing campaign. 

Setup : 
Define the rootDir in the yml files to point to the directory where the servers will store their data.
The directory structure below is directories corresponding to list of databases and within each database are directories corresponding to the tables in the database. 
The table directory in turn will contain one file per column defined in the table metadata.
Usage : 

Server is the main class. 
Multiple server instances can be instantiated - Call each with the complete path of the test.xml or test1.xml file.
The test and test1.xml files contain the configuration of the rest service used to handle the queries.
Adjust the path entries in these files to point to the configuration and configuration1.yml files. 


The data is laid out on the nodes in the cluster in column format. A separate file is created per column. 
The position in the column file determines which row the various column values belong to. 

Dependencies
1. Requires Zookeeper - the nodes will bind to zookeeper under /columnDB 
2. The client picks up the servers from zookeeper and gets the name , host and port values to connect to.

Security notes
1. The REST API has no authentication. The sample yml files bind the application and admin connectors to 127.0.0.1 (`bindHost`). Only change `bindHost` to a reachable address on a trusted, firewalled network. The node registers itself in ZooKeeper under `messages.advertisedHost` if set, otherwise under `bindHost` (or this machine's host name when `bindHost` is `0.0.0.0`), so set `advertisedHost` to the name other machines should use.
2. Cluster, database, table and column names may only contain letters, digits, `_` and `-` (max 128 characters), because they are used as directory and file names. Requests with other names, or with a `clusterName` that does not match the server's configured cluster, are rejected with HTTP 400. So are column definitions whose key differs from their `columnName` or whose `maxSize` is outside 1–65536, and batches or queries that name a table or column that doesn't exist.
