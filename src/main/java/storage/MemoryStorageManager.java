package storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryStorageManager {

    Map<String,Map<String,Map<String, List<String>>>> clusterData = new HashMap<>();


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
        clusterData.get(databaseName).get(tableName).computeIfAbsent(columnName, c->new ArrayList<>());
    }

    public void deleteDatabase(String databaseName)
    {
        clusterData.remove(databaseName);
    }

    public void deleteTable(String databaseName, String tableName)
    {
        clusterData.get(databaseName).remove(tableName);
    }

    public void removeolumn(String databaseName, String tableName, String columnName)
    {
        clusterData.get(databaseName).get(tableName).remove(columnName);
    }

    //TODO - add metadata
    // query metadata
    // query data




}
