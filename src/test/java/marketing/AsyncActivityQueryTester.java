package marketing;

import client.AsyncColumnDBClient;
import client.ColumnDBClient;
import query.ConditionType;
import query.CountRequest;
import query.Criteria;
import query.DataContainer;
import table.Table;

import java.util.List;

public class AsyncActivityQueryTester {

    public static void main(String[] args)  throws Exception {


        AsyncColumnDBClient client = AsyncColumnDBClient.getInstance();



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

           List<DataContainer> dataContainers = client.queryData(countRequest).get();

            //dataContainers.stream().forEach(System.out::println);

            Table table = new Table();

            table.process(dataContainers);

            System.out.println(table);





    }

}
