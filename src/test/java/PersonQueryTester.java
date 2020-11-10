import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.*;

public class PersonQueryTester {

    public static void main(String[] args)  throws Exception {

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";

        ColumnDBClient client = ColumnDBClient.getInstance();

        client.addCluster(clusterName,10005);
        client.addCluster(clusterName,"localhost");
        client.addCluster(clusterName1,10015);
        client.addCluster(clusterName1,"localhost");


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

            Response response = client.query(clusterName,countRequest);

            System.out.println(response.getResult());

            DataResponse response1 = client.queryData(clusterName,countRequest);
            System.out.println(response1.getValues());





    }

}
