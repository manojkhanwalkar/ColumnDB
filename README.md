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