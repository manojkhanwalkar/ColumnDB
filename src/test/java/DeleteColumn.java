import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.ColumnMetaData;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class DeleteColumn {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {



        ColumnDBClient client = ColumnDBClient.getInstance();


        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.DeleteColumn);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName("demo1");
            tableMetaData.setTableName("person1");

            //age,gender,zip,income,state
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("DL");
                metadata.setMaxSize(16);
                tableMetaData.addColumn(metadata);

            }
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("SSN");
                metadata.setMaxSize(9);
                tableMetaData.addColumn(metadata);

            }

            meta.setMetaData(tableMetaData);

            client.send(meta);




        }



    }

}
