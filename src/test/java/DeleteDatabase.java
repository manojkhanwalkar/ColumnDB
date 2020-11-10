import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class DeleteDatabase {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";

        ColumnDBClient client = ColumnDBClient.getInstance();

        client.addCluster(clusterName,"localhost", 10005);
        client.addCluster(clusterName1,"localhost",10015);


        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.DeleteDatabase);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName("demo1");

            meta.setMetaData(tableMetaData);

            client.send(meta);




        }



    }

}
