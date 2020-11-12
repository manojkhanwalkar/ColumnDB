package marketing;

import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import query.MetaRequest;
import query.MetaRequestType;
import query.TableMetaData;

public class TestDatabaseMetaData {




    @Test
    public void create()
    {

        ColumnDBClient client = ColumnDBClient.getInstance();

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.CreateDatabase);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setDatabaseName("marketing");

            meta.setMetaData(tableMetaData);

            client.send(meta);


    }

}
