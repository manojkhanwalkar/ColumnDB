import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.ColumnMetaData;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class DeleteTable {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {


        ColumnDBClient client = ColumnDBClient.getInstance();



        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.DeleteTable);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName("demo1");
            tableMetaData.setTableName("person1");

            meta.setMetaData(tableMetaData);

            client.send(meta);




        }



    }

}
