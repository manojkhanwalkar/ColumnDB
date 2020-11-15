package query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableMetaData {

    String clusterName ;
    String databaseName;
    String tableName ;

    Map<String,ColumnMetaData> columns = new HashMap<>();


    public static TableMetaData addColumns(TableMetaData orig, TableMetaData delta)
    {
        TableMetaData combined = new TableMetaData();
        combined.clusterName = orig.clusterName;
        combined.databaseName = orig.databaseName;
        combined.tableName = orig.tableName;

        combined.columns.putAll(orig.columns);

        combined.columns.putAll(delta.columns);

        return combined;
    }

    public static TableMetaData removeColumns(TableMetaData orig, TableMetaData delta)
    {
        TableMetaData combined = new TableMetaData();
        combined.clusterName = orig.clusterName;
        combined.databaseName = orig.databaseName;
        combined.tableName = orig.tableName;

        combined.columns.putAll(orig.columns);

        delta.columns.keySet().forEach(key->{

            combined.columns.remove(key);

        });

        return combined;
    }

    static ObjectMapper mapper = new ObjectMapper();


    public static TableMetaData fromJSONString(String str)
    {
        try {
            //  FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+tableName+".meta");

            BufferedReader metaFileReader = new BufferedReader(new FileReader(str));
            String s = metaFileReader.readLine();

            return mapper.readValue(s, TableMetaData.class);


        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;

    }


    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void addColumn(ColumnMetaData metadata)
    {
        columns.put(metadata.getColumnName(), metadata);
    }

    public Map<String, ColumnMetaData> getColumns() {
        return columns;
    }

    public  ColumnMetaData getColumnMetaData(String name) {
        return columns.get(name);
    }


    public void setColumns(Map<String, ColumnMetaData> columns) {
        this.columns = columns;
    }


    @Override
    public String toString() {
        return "TableMetaData{" +
                " databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
