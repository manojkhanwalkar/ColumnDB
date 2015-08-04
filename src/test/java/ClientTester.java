import client.ColumnDBClient;
import org.codehaus.jackson.map.ObjectMapper;
import query.*;

public class ClientTester {


    static ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args)  throws Exception {

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";

 /*       for (int j=0;j<1;j++) {

            Thread t = new Thread(()-> {


            for (int i = 0; i < 1; i++) {
                GraphDBClient client = GraphDBClient.getInstance();
                Request request = new Request();
                request.setId("DP5");
                request.setOperation(DBOperation.Query);
                request.setDbName("db2");

                //  ObjectMapper mapper = new ObjectMapper();

                // String s = mapper.writeValueAsString(request);

                //System.out.println(s);

//
                Response response = client.send(request);

                System.out.println(response);
            }
            });

            t.start();

        }*/

        // GraphDB db = ((DBService) Server.getService("DBService")).getDatabase("db1");
        ColumnDBClient client = ColumnDBClient.getInstance();

        client.addCluster(clusterName,10005);
        client.addCluster(clusterName,"localhost");
        client.addCluster(clusterName1,10015);
        client.addCluster(clusterName1,"localhost");

   /*     Set<String> ids = new HashSet<>();


        for (int j=0;j<1;j++) {

            String clusterToUse ;
            if (j%2==0)
                clusterToUse = clusterName;
            else
                clusterToUse = clusterName1;

            try {
                BufferedReader reader = new BufferedReader(new FileReader("/Users/mkhanwalkar/test/data/input.txt"));

                String s = null;
                while ((s = reader.readLine()) != null) {
                    //  System.out.println(s);
                    String[] sA = s.split(",");

                    Request request = new Request();
                    request.setId(sA[0]);
                    ids.add(sA[0]);
                    // request.setName(sA[0]);
                    request.setOperation(DBOperation.AddNode);
                    request.setDbName("db1");
                    Response response = client.send(clusterToUse, request);
                    System.out.println(response);

                    for (int i = 1; i < sA.length; i++) {
                        //                 Node child = db.createOrGetNode( sA[i]);
                        Request child = new Request();
                        child.setId(sA[i]);
                        ids.add(sA[i]);
                        // child.setName(sA[i]);
                        child.setOperation(DBOperation.AddNode);
                        child.setDbName("db1");
                        response = client.send(clusterToUse, child);
                        System.out.println(response);

                        Request relation = new Request();
                        relation.setId(sA[0]);
                        relation.setTgtId(sA[i]);
                        relation.setOperation(DBOperation.AddRelation);
                        relation.setDbName("db1");
                        response = client.send(clusterToUse, relation);
                        System.out.println(response);

                    }


                }

            } catch (IOException e) {
                e.printStackTrace();
            }*/

         /*   ids.stream().forEach(s->{
                Request child = new Request();
                child.setId(s);
                child.setOperation(DBOperation.DeleteNode);
                child.setDbName("db1");
                client.send(clusterToUse, child);

            });*/
        {

            MetaRequest meta = new MetaRequest();
            meta.setType(MetaRequestType.CreateTable);

            TableMetaData tableMetaData = new TableMetaData();
            tableMetaData.setClusterName("cluster1");
            tableMetaData.setDatabaseName("demo");
            tableMetaData.setTableName("person");

            //age,gender,zip,income,state
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("age");
                metadata.setMaxSize(2);
                tableMetaData.addColumn(metadata);

            }
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("gender");
                metadata.setMaxSize(1);
                tableMetaData.addColumn(metadata);

            }
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("zip");
                metadata.setMaxSize(5);
                tableMetaData.addColumn(metadata);

            }
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("income");
                metadata.setMaxSize(7);
                tableMetaData.addColumn(metadata);

            }
            {
                ColumnMetaData metadata = new ColumnMetaData();
                metadata.setColumnName("state");
                metadata.setMaxSize(2);
                tableMetaData.addColumn(metadata);

            }

            meta.setMetaData(tableMetaData);


      /*      Response response = client.send(clusterName, meta);
            System.out.println(response); */

            MetaResponse metaResponse = client.query(clusterName);

            System.out.println(mapper.writeValueAsString(metaResponse));


        }



    }

}
