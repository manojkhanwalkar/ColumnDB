package marketing;

import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import person.DeleteDatabase;
import query.ColumnMetaData;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class TestDatabaseMetaData {


    static final String databaseName = "marketing";

    static final String tableName = "activity";


    @Test
    public void createDatabase()
    {

        createOrDeleteDatabase(MetaRequestType.CreateDatabase,databaseName);


    }

    @Test
    public void deleteDatabase()
    {

        createOrDeleteDatabase(MetaRequestType.DeleteDatabase,databaseName);


    }

    @Test
    public void deleteTable()
    {

        ColumnDBClient client = ColumnDBClient.getInstance();
        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.DeleteTable);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName(databaseName);
            tableMetaData.setTableName(tableName);

            meta.setMetaData(tableMetaData);

            client.send(meta);




        }

    }


    private void createOrDeleteDatabase(MetaRequestType type, String databaseName)
    {
        switch(type)
        {
            case CreateDatabase:
            case DeleteDatabase:
                ColumnDBClient client = ColumnDBClient.getInstance();

                MetaRequest meta = new MetaRequest();
                TableMetaData tableMetaData = new TableMetaData();
                tableMetaData.setDatabaseName(databaseName);
                meta.setType(type);

                meta.setMetaData(tableMetaData);

                client.send(meta);

                 break;

            default :
                throw new RuntimeException("Unexpected request type");
        }
    }


    @Test
    public void createTable()
    {
        ColumnDBClient client = ColumnDBClient.getInstance();

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.CreateTable);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName(databaseName);
            tableMetaData.setTableName(tableName);

            String[] columns = { "userid" , "campaignid" , "clickid" , "seen" , "clicked" , "purchased" } ;
            int[] sizes = { 10, 20, 36, 1,1,1 } ;

            for (int i=0;i<columns.length;i++) {

                addColumn(columns[i], sizes[i], tableMetaData);
            }


            meta.setMetaData(tableMetaData);

            client.send(meta);




        }


    @Test public void addColumn() {

        addoRDeleteColumn(MetaRequestType.AddColumn);
    }

    @Test public void deleteColumn() {
        addoRDeleteColumn(MetaRequestType.DeleteColumn);


    }

        private void addoRDeleteColumn(MetaRequestType type)
        {

            ColumnDBClient client = ColumnDBClient.getInstance();

            String[] columns = { "test" , "test1"  } ;
            int[] sizes = { 10, 20 } ;

                MetaRequest meta = new MetaRequest();
                meta.setType(type);

                TableMetaData tableMetaData = new TableMetaData();
                tableMetaData.setDatabaseName(databaseName);
                tableMetaData.setTableName(tableName);


            for (int i=0;i<columns.length;i++) {

                addColumn(columns[i], sizes[i], tableMetaData);
            }


            meta.setMetaData(tableMetaData);

                client.send(meta);




            }




        private void addColumn(String name , int size , TableMetaData tableMetaData) {

            ColumnMetaData metadata = new ColumnMetaData();
            metadata.setColumnName(name);
            metadata.setMaxSize(size);
            tableMetaData.addColumn(metadata);


        }



}
