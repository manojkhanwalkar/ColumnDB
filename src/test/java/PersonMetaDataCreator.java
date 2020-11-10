import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.*;

public class PersonMetaDataCreator {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";

        ColumnDBClient client = ColumnDBClient.getInstance();

        client.addCluster(clusterName,10005);
        client.addCluster(clusterName,"localhost");
        client.addCluster(clusterName1,10015);
        client.addCluster(clusterName1,"localhost");


        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.CreateTable);

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

            meta.setMetaData(tableMetaData);

            client.send("cluster1",meta);




        }



    }

}
