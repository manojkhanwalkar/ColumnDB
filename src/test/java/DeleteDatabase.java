import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class DeleteDatabase {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {



        ColumnDBClient client = ColumnDBClient.getInstance();



        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.DeleteDatabase);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName("demo");

            meta.setMetaData(tableMetaData);

            client.send(meta);




        }



    }

}
