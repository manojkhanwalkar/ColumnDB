import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.ClusterMetaData;
import query.DatabaseMetaData;
import query.TableMetaData;

import java.io.*;

/**
 * Created by mkhanwalkar on 8/4/15.
 */
public class QuickTester {

    static final String seperator = "/";
    static final String rootDirName = "/tmp";
    static final String clusterName = "cluster1";

    static ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) {

        File dir = new File(rootDirName+seperator+clusterName);

        ClusterMetaData clusterMD = new ClusterMetaData();
        clusterMD.setName(clusterName);

        for (File f : dir.listFiles()) {
            if (f.isDirectory())
            {
                processDatabase(f,clusterMD);
            }


        }

        try {
            String s = mapper.writeValueAsString(clusterMD);
            System.out.println(s);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }


    }

    private static void processDatabase(File database, ClusterMetaData clusterMD) {

        DatabaseMetaData databaseMD = new DatabaseMetaData();
        databaseMD.setName(database.getName());

        clusterMD.addDatabaseMetaData(databaseMD);

      //  System.out.println(database.getName());
        for (File f : database.listFiles())
        {
            if (f.isDirectory())
            {
                processTable(f,databaseMD);
            }

        }
    }

    private static void processTable(File table, DatabaseMetaData databaseMD) {

       // System.out.println(table.getName());

        String[] metaFile = table.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {

                if (name.endsWith(".meta"))
                    return true;
                else
                    return false;

            }
        });

        TableMetaData tableMetaData = null;
        try {
            FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+databaseMD.getName()+seperator+table.getName()+seperator+table.getName()+".meta");
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s = metaFileReader.readLine();

            tableMetaData = mapper.readValue(s, TableMetaData.class);
            tableMetaData.setTableName(table.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // System.out.println(tableMetaData);


        // read meta file into
        databaseMD.addTableMetaData(tableMetaData);


    }

  }
