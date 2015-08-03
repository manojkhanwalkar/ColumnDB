package loader;


import com.fasterxml.jackson.databind.ObjectMapper;
import query.TableMetaData;

import java.io.*;

public class CSVLoader {

    String rootDirName ;
    String clusterName;
    String databaseName ;
    String tableName ;

    static final String seperator = "/";

    public CSVLoader(String rootDirName, String clusterName, String databaseName, String tableName, String fileName) {
        this.rootDirName = rootDirName;
        this.clusterName = clusterName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static void main(String[] args) {

        CSVLoader loader = new CSVLoader("/tmp","cluster1","demo","person","/tmp/person.csv");

        loader.load();

        loader.loadMetaData();

    }

    static ObjectMapper mapper = new ObjectMapper();

    private void loadMetaData()
    {
        try {
            FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s = metaFileReader.readLine();

            TableMetaData tableMetaData = mapper.readValue(s,TableMetaData.class);

            System.out.println(tableMetaData);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private void load() {

        //TODO - move the dir creation part to the create data base and create table calls .

        File dir = new File(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName);
        dir.mkdirs();

        //TODO - load person.meta and use that to create the columnar files .
    }


}
