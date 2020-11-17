package storage;

import org.apache.commons.lang.StringUtils;
import query.ClusterMetaData;
import query.Request;
import query.TableMetaData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryStorageManager implements StorageManager {

    Map<String,Map<String,Map<String,String>>> clusterData = new HashMap<>();

    //TODO - implement map for Metadata
    //TODO - update metadata on column add and delete


    public void createDatabase(String databaseName)
    {
        clusterData.computeIfAbsent(databaseName, k->new HashMap<>());
    }

    public void createTable(String databaseName, String tableName)
    {
        clusterData.get(databaseName).computeIfAbsent(tableName, t->new HashMap<>());
    }

    public void addColumn(String databaseName, String tableName, String columnName)
    {
        clusterData.get(databaseName).get(tableName).computeIfAbsent(columnName, c->StringUtils.EMPTY);
    }

    public void deleteDatabase(String databaseName)
    {
        clusterData.remove(databaseName);
    }

    public void deleteTable(String databaseName, String tableName)
    {
        clusterData.get(databaseName).remove(tableName);
    }

    public void removeColumn(String databaseName, String tableName, String columnName)
    {
        clusterData.get(databaseName).get(tableName).remove(columnName);
    }


    @Override
    public boolean existsDatabase(String databaseName) {
        return false;
    }

    @Override
    public void deleteColumn(String databaseName, String tableName, TableMetaData columnsToBeDeleted) {

    }

    @Override
    public void addColumn(String databaseName, String tableName, TableMetaData tableMetaData) {

    }

    @Override
    public void write(Request request) {

    }

    @Override
    public void populateClusterMetaData(ClusterMetaData clusterMD) {

    }
}
