package loader;


import com.fasterxml.jackson.databind.ObjectMapper;
import query.ColumnMetaData;
import query.TableMetaData;

import java.io.*;
import java.util.Collection;

public class CSVLoader {

    String rootDirName ;
    String clusterName;
    String databaseName ;
    String tableName ;
    String dataFileName;

    static final String seperator = "/";

    public CSVLoader(String rootDirName, String clusterName, String databaseName, String tableName, String fileName) {
        this.rootDirName = rootDirName;
        this.clusterName = clusterName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.dataFileName = fileName;
    }

    public static void main(String[] args) {

        CSVLoader loader = new CSVLoader("/tmp","cluster1","demo","person","/tmp/person.csv");

        loader.loadMetaData();
        loader.loadData();

    }

    static ObjectMapper mapper = new ObjectMapper();
    TableMetaData tableMetaData=null;

    private void loadMetaData()
    {
        try {
            FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s = metaFileReader.readLine();

             tableMetaData = mapper.readValue(s,TableMetaData.class);

            System.out.println(tableMetaData);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void loadData() {

    /*    //TODO - move the dir and file creation part to the create data base and create table calls .

        File dir = new File(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName);
        dir.mkdirs();

        tableMetaData.getColumns().values().stream().forEach(cmd->{


            File file = new File(dir+seperator+cmd.getColumnName());
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        */

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
                    writers[i].write(values[i]);  //TODO - adjust to size later
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


}
