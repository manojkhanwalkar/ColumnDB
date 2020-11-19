package marketing;

import client.AsyncColumnDBClient;
import client.ColumnDBClient;
import query.ConditionType;
import query.CountRequest;
import query.Criteria;
import query.DataContainer;
import table.Table;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

          var dataContainer= client.queryData(countRequest).get();
            Table table = new Table();
            table.process(dataContainer);
            System.out.println(table);

          client.queryData(countRequest,(dc)->{
               Table t = new Table();

                t.process(dc);

                System.out.println(t);
            });

            System.out.println("Using Async consumer callback");


           Thread.sleep(5000);

    }

}
