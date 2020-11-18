package storage;

import query.ClusterMetaData;
import query.Request;
import query.TableMetaData;

public interface StorageManager {

    default void  setRootDirName(String rootDirName) {

    }

    default void setClusterName(String clusterName)
    {

    }

    void createDatabase(String databaseName);

    boolean existsDatabase(String databaseName);

    void deleteDatabase(String databaseName);

    void deleteTable(String databaseName, String tableName);

    void deleteColumn(String databaseName, String tableName, TableMetaData columnsToBeDeleted);

    void addColumn(String databaseName, String tableName, TableMetaData tableMetaData);

    void createTable(String databaseName, String tableName, TableMetaData tableMetaData);

    void write(Request request);

    void populateClusterMetaData(ClusterMetaData clusterMD);
}
