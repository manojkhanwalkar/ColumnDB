package storage;

import org.apache.commons.lang.StringUtils;
import query.*;
import rest.DBLocks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryStorageManager implements StorageManager {

    Map<String,Map<String,Map<String,String>>> clusterData = new HashMap<>();

    Map<String,Map<String,TableMetaData>> metaData = new HashMap<>();


    public void createDatabase(String databaseName)
    {

        clusterData.computeIfAbsent(databaseName, k->new HashMap<>());
        metaData.computeIfAbsent(databaseName, k->new HashMap<>());
    }

    @Override
    public boolean existsTable(String databaseName, String tableName) {
        return clusterData.get(databaseName)!=null && clusterData.get(databaseName).get(tableName)!=null;
    }

    public void createTable(String databaseName, String tableName, TableMetaData tableMetaData)
    {
        clusterData.get(databaseName).computeIfAbsent(tableName, t->new HashMap<>());
        metaData.get(databaseName).computeIfAbsent(tableName, t->tableMetaData);
        addColumn(databaseName,tableName,tableMetaData);
    }



    public void deleteDatabase(String databaseName)
    {
        clusterData.remove(databaseName);
        metaData.remove(databaseName);
    }

    public void deleteTable(String databaseName, String tableName)
    {

        clusterData.get(databaseName).remove(tableName);
        metaData.get(databaseName).remove(tableName);
    }



    @Override
    public boolean existsDatabase(String databaseName) {
        return clusterData.containsKey(databaseName);
    }

    @Override
    public void deleteColumn(String databaseName, String tableName, TableMetaData columnsToBeDeleted) {

        columnsToBeDeleted.getColumns().values().stream().forEach(cmd->{

            clusterData.get(databaseName).get(tableName).remove(cmd.getColumnName());
        });

        TableMetaData combined = TableMetaData.removeColumns(metaData.get(databaseName).get(tableName), columnsToBeDeleted);
        metaData.get(databaseName).put(tableName,combined);

    }

    @Override
    public void addColumn(String databaseName, String tableName, TableMetaData tableMetaData) {

        tableMetaData.getColumns().values().stream().forEach(cmd -> {

                    clusterData.get(databaseName).get(tableName).computeIfAbsent(cmd.getColumnName(), c -> StringUtils.EMPTY);
                });

        TableMetaData combined = TableMetaData.addColumns(metaData.get(databaseName).get(tableName), tableMetaData);
        metaData.get(databaseName).put(tableName,combined);


    }

    @Override
    public void write(Request request) {
        DBLocks dbLocks = DBLocks.getInstance();
        String databaseName = request.getDatabaseName();
        String tableName = request.getTableName();

        try {


            dbLocks.lock(databaseName,tableName, DBLocks.Type.Write);


            DataContainer container = request.getDataContainer();
            container.getValues().entrySet().stream().forEach(ent->{

                String col = ent.getKey();

                StringBuilder value = new StringBuilder(clusterData.get(databaseName).get(tableName).get(col));

                for (String s : ent.getValue())
                {
                    value.append(s);
                }

                clusterData.get(databaseName).get(tableName).put(col,value.toString());

            });

        } finally {
            dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);
        }

    }

    @Override
    public Response processCount(CountRequest request) {
        return null;
    }

    @Override
    public DataContainer processData(CountRequest request) {
        return null;
    }

    @Override
    public void populateClusterMetaData(ClusterMetaData clusterMD) {

        metaData.keySet().stream().forEach(database->{
            processDatabase(clusterMD,database);
        });
    }


    private  void processDatabase(ClusterMetaData clusterMD, String databaseName) {

        DatabaseMetaData databaseMD = new DatabaseMetaData();
        databaseMD.setName(databaseName);

        clusterMD.addDatabaseMetaData(databaseMD);

        metaData.get(databaseName).keySet().forEach(table->{
            processTable(databaseMD,table);
        });


    }

    private  void processTable( DatabaseMetaData databaseMD, String tableName) {

            TableMetaData tableMetaData = metaData.get(databaseMD.getName()).get(tableName);
            databaseMD.addTableMetaData(tableMetaData);

    }

}
