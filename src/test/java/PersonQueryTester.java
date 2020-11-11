import client.ColumnDBClient;
import query.*;

import java.util.List;

public class PersonQueryTester {

    public static void main(String[] args)  throws Exception {


        ColumnDBClient client = ColumnDBClient.getInstance();



            CountRequest countRequest = new CountRequest();
            countRequest.setClusterName("cluster1");
            countRequest.setDatabaseName("demo");
            countRequest.setTableName("person");

            {
                Criteria criteria = new Criteria();
                criteria.setColumnName("age");
                criteria.setType(ConditionType.GT);
                criteria.setRhs("25");

                countRequest.addCriteria(criteria);
            }
            {
                Criteria criteria = new Criteria();
                criteria.setColumnName("gender");
                criteria.setType(ConditionType.EQ);
                criteria.setRhs("M");

                countRequest.addCriteria(criteria);

            }

            List<Response> responses = client.query(countRequest);

            responses.stream().map(resp->resp.getResult()).forEach(System.out::println);

            List<DataContainer> dataContainers = client.queryData(countRequest);

            dataContainers.stream().forEach(System.out::println);





    }

}
