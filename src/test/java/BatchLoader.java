import client.ColumnDBClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;

import java.io.*;

public class BatchLoader {



  String  fileName;
  String databaseName;
  String tableName;

    public BatchLoader(String fileName, String databaseName , String tableName) {

        this.fileName = fileName;
        this.databaseName=databaseName;
        this.tableName = tableName;
    }

    public static void main(String[] args) {

        BatchLoader loader = new BatchLoader("/tmp/person.csv","demo","person");

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";

        ColumnDBClient client = ColumnDBClient.getInstance();

        client.addCluster(clusterName,"localhost", 10005);
        client.addCluster(clusterName1,"localhost",10015);

        MetaResponse metaResponse = client.query();

        System.out.println(metaResponse);

        Request request  = new Request();

        request.setClusterName(clusterName);
        request.setDatabaseName("demo");
        request.setTableName("person");

        DataContainer container = new DataContainer(); // to be filled from the file .

        request.setDataContainer(container);

        client.send(request);



    }

    static ObjectMapper mapper = new ObjectMapper();
    TableMetaData tableMetaData=null;

    String[] spaces;

   /* private void loadMetaData()
    {
        try {
            FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s = metaFileReader.readLine();

             tableMetaData = mapper.readValue(s,TableMetaData.class);

            System.out.println(tableMetaData);

            int maxSpaces =0;

            for (ColumnMetaData cmd :  tableMetaData.getColumns().values())
            {
                int len = cmd.getMaxSize();
                if (len>maxSpaces) maxSpaces = len;

            }

            System.out.println("Max length is " + maxSpaces);

            spaces = new String[maxSpaces];
            String s1 = "";
            for (int i=0;i<maxSpaces;i++)
            {
                spaces[i] = s1;
                s1=s1+" ";
            }



            } catch (IOException e) {
            e.printStackTrace();
        }


    }*/

   /* private void loadData() {


        // read file for columns , get maxsize from metadata .
        // open handles for all files for writing
        // for each record - get maxsize padded string for each column
        // write in each file
        try {
            FileReader reader = new FileReader(dataFileName);
            BufferedReader dataReader = new BufferedReader(reader);
            String[] columnNames  = dataReader.readLine().split(",");
            BufferedWriter[] writers = new BufferedWriter[columnNames.length];
            int[] lengths = new int[columnNames.length];

            createWriters(writers,columnNames,lengths);

            String s;
            while ((s=dataReader.readLine())!=null)
            {
                String[] values = s.split(",");
                if (values.length!=columnNames.length)
                {
                    System.out.println("Invalid record data " + s);
                    continue;
                }
                for (int i=0;i<values.length;i++)
                {
                    writers[i].write(adjustLength(values[i],lengths[i]));
                }
            }

            for (BufferedWriter writer : writers )
            {
                writer.flush();
                writer.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private String adjustLength(String s, int maxLength)
    {
        try {
            return s + spaces[maxLength - s.length()];
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    //TODO - change code to go to end of file . might have to use different writer .

    private void createWriters(BufferedWriter[] writers, String[] columnNames, int[] lengths)
    {
        try {
            File dir = new File(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName);
            for (int i=0;i<columnNames.length;i++)
            {
                String name = columnNames[i];
                ColumnMetaData cmd = tableMetaData.getColumnMetaData(name);
                File file = new File(dir+seperator+cmd.getColumnName());
                writers[i] = new BufferedWriter(new FileWriter(file));
                lengths[i] = cmd.getMaxSize();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

*/
}
