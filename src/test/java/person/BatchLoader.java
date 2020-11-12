package person;

import client.ColumnDBClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BatchLoader {



  String  dataFileName;
  String databaseName;
  String tableName;

    ColumnDBClient client;

    public BatchLoader(String fileName, String databaseName , String tableName) {

        this.dataFileName = fileName;
        this.databaseName=databaseName;
        this.tableName = tableName;

         client = ColumnDBClient.getInstance();


    }

    public static void main(String[] args) {

        BatchLoader loader = new BatchLoader("/tmp/person.csv", "demo", "person");


        loader.loadMetaData();

        DataContainer datacontainer = loader.loadData();

        loader.load(datacontainer);

    }

    public void load(DataContainer container)
    {



        Request request  = new Request();


        request.setDatabaseName(databaseName);
        request.setTableName(tableName);


        request.setDataContainer(container);

        client.send(request);





    }

    static ObjectMapper mapper = new ObjectMapper();
    TableMetaData tableMetaData=null;

    String[] spaces;

    static String rootDirName = "/tmp";
    static String seperator = "/";

    String clusterName = "cluster1";
    String clusterName1 = "cluster2";


    public void loadMetaData()
    {


            MetaResponse metaResponse = client.query();

            System.out.println(metaResponse);

            tableMetaData= metaResponse.getClusterMetaData().getDatabaseMetaData().stream()
                    .filter(d->d.getName().equals(databaseName))
                    .flatMap(d->d.getTableMetaData().stream())
                    .filter(t->t.getTableName().equals(tableName)).findFirst().get();


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





    }



    public DataContainer loadData() {


        // read file for columns , get maxsize from metadata .
        // open handles for all files for writing
        // for each record - get maxsize padded string for each column
        // write in each file
        try {
            FileReader reader = new FileReader(dataFileName);
            BufferedReader dataReader = new BufferedReader(reader);
            String[] columnNames  = dataReader.readLine().split(",");
            List<List<String>>writers = new ArrayList<>();
            int[] lengths = new int[columnNames.length];

            //createWriters(writers,columnNames,lengths);
            createLists(writers,columnNames,lengths);

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
                    writers.get(i).add(adjustLength(values[i],lengths[i]));
                }
            }

            return createContainer(writers,columnNames);



        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;


    }

    private DataContainer createContainer(List<List<String>> writers , String[] columnNames) {

        DataContainer container = new DataContainer();

        for (int i=0;i<columnNames.length;i++)
        {
            container.addValues(columnNames[i], writers.get(i));
        }

        return container;

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




    private void createLists(List<List<String>> writers, String[] columnNames, int[] lengths)
    {
            for (int i=0;i<columnNames.length;i++)
            {
                String name = columnNames[i];
                ColumnMetaData cmd = tableMetaData.getColumnMetaData(name);
                lengths[i] = cmd.getMaxSize();
                writers.add(new ArrayList<>());

            }
    }



}
