package query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class TableMetaData {

    String clusterName ;
    String databaseName;
    String tableName ;

    List<ColumnMetaData>  columns = new ArrayList<ColumnMetaData>();

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
        columns.add(metadata);
    }

    public List<ColumnMetaData> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMetaData> columns) {
        this.columns = columns;
    }


    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper();
        TableMetaData tableMetaData = new TableMetaData();
        tableMetaData.setClusterName("cluster1");
        tableMetaData.setDatabaseName("demo");
        tableMetaData.setTableName("person");

        //age,gender,zip,income,state
        {
            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName("age");
            metadata.setMaxSize(2);
            tableMetaData.addColumn(metadata);

        }
        {
            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName("gender");
            metadata.setMaxSize(1);
            tableMetaData.addColumn(metadata);

        }
        {
            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName("zip");
            metadata.setMaxSize(5);
            tableMetaData.addColumn(metadata);

        }
        {
            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName("income");
            metadata.setMaxSize(7);
            tableMetaData.addColumn(metadata);

        }
        {
            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName("state");
            metadata.setMaxSize(2);
            tableMetaData.addColumn(metadata);

        }

        try {
            String s = mapper.writeValueAsString(tableMetaData);
            System.out.println(s);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

    }

}
