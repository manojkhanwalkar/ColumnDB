package marketing;

import client.ColumnDBClient;
import query.*;
import table.Table;

import java.util.List;

public class ActivitytQueryTester {

    public static void main(String[] args)  throws Exception {


        ColumnDBClient client = ColumnDBClient.getInstance();



            CountRequest countRequest = new CountRequest();

            countRequest.setDatabaseName("marketing");
            countRequest.setTableName("activity");

            {
                Criteria criteria = new Criteria();
                criteria.setColumnName("clicked");
                criteria.setType(ConditionType.EQ);
                criteria.setRhs("Y");

                countRequest.addCriteria(criteria);
            }



        /*    List<Response> responses = client.query(countRequest);

            responses.stream().map(resp->resp.getResult()).forEach(System.out::println);*/

           List<DataContainer> dataContainers = client.queryData(countRequest);

            //dataContainers.stream().forEach(System.out::println);

            Table table = new Table();

            table.process(dataContainers);

            System.out.println(table);





    }

}
