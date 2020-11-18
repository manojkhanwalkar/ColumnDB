package storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;
import rest.CountNDataProcessorForFile;
import rest.DBLocks;
import rest.DataWriter;

import java.io.*;

import static rest.ColumnResource.seperator;

public class FileStorageManager implements StorageManager {

    String rootDirName;

    @Override
    public void setRootDirName(String rootDirName) {

        this.rootDirName = rootDirName;
    }

    String clusterName;
    @Override
    public void setClusterName(String clusterName) {

        this.clusterName = clusterName;

    }

    @Override
    public void createDatabase(String databaseName) {
        String databasePath = rootDirName+seperator+clusterName+seperator+databaseName;
        if (!exists(databasePath)) {

            createDirs(clusterName, databaseName, null);
        }




    }

    @Override
    public boolean existsTable(String databaseName, String tableName) {
        String tablePath = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName;

        return exists(tablePath);

    }

    @Override
    public boolean existsDatabase(String databaseName) {
        String tablePath = rootDirName+seperator+clusterName+seperator+databaseName;
        return exists(tablePath);

    }

    @Override
    public void deleteDatabase(String databaseName) {
        deleteDatabaseDirs(clusterName,databaseName);
    }

    @Override
    public void deleteTable(String databaseName, String tableName) {
        deleteTableDir(clusterName,databaseName,tableName);


    }

    @Override
    public Response processCount(CountRequest request) {
        CountNDataProcessorForFile processor = new CountNDataProcessorForFile(request,this);

        return processor.processCount();
    }

    @Override
    public DataContainer processData(CountRequest request) {

        CountNDataProcessorForFile processor = new CountNDataProcessorForFile(request,this);

        return processor.processData();

    }

    @Override
    public void deleteColumn(String databaseName, String tableName, TableMetaData columnsToBeDeleted) {


                String metaFile = rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta";


                TableMetaData combined = TableMetaData.removeColumns(TableMetaData.fromJSONString(metaFile), columnsToBeDeleted);

                try {
                    String s = mapper.writeValueAsString(combined);

                    FileWriter writer = new FileWriter(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta");
                    BufferedWriter metaFileWriter = new BufferedWriter(writer);
                    metaFileWriter.write(s);
                    metaFileWriter.flush();
                    metaFileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                File dir = new File(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName);
                columnsToBeDeleted.getColumns().values().stream().forEach(cmd -> {


                    File file = new File(dir + seperator + cmd.getColumnName());
                    file.delete();
                });



    }

    @Override
    public void addColumn(String databaseName, String tableName, TableMetaData tableMetaData) {


                createDirs(clusterName, databaseName, tableName);
                File dir = new File(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName);
                tableMetaData.getColumns().values().stream().forEach(cmd -> {


                    File file = new File(dir + seperator + cmd.getColumnName());
                    try {
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                String metaFile = rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta";

                TableMetaData combined = TableMetaData.addColumns(TableMetaData.fromJSONString(metaFile), tableMetaData);

                try {
                    String s = mapper.writeValueAsString(combined);

                    FileWriter writer = new FileWriter(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta");
                    BufferedWriter metaFileWriter = new BufferedWriter(writer);
                    metaFileWriter.write(s);
                    metaFileWriter.flush();
                    metaFileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }



    }

    static ObjectMapper mapper = new ObjectMapper();


    @Override
    public void createTable(String databaseName, String tableName, TableMetaData tableMetaData) {

        String tablePath = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName;

        if (!exists(tablePath)) {


            createDirs(clusterName, databaseName, tableName);

            File dir = new File(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName);
            //dir.mkdirs();

            tableMetaData.getColumns().values().stream().forEach(cmd -> {


                File file = new File(dir + seperator + cmd.getColumnName());
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            try {
                String s = mapper.writeValueAsString(tableMetaData);

                FileWriter writer = new FileWriter(rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta");
                BufferedWriter metaFileWriter = new BufferedWriter(writer);
                metaFileWriter.write(s);
                metaFileWriter.flush();
                metaFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }


    }





    @Override
    public void write(Request request) {

        DataWriter writer = new DataWriter(request,rootDirName);

        writer.write();

    }

    @Override
    public void populateClusterMetaData(ClusterMetaData clusterMD) {



        File dir = new File(rootDirName+seperator+clusterName);


        for (File f : dir.listFiles()) {
            if (f.isDirectory())
            {
                processDatabase(f,clusterMD,clusterName);
            }


        }

        try {
            String s = mapper.writeValueAsString(clusterMD);
            System.out.println(s);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }





    }


    private boolean exists(String dir)
    {
        File dirFile = new File(dir);
        if (dirFile.exists())
            return true;
        else
            return false;
    }

    private void createDirs(String clusterName, String databaseName, String tableName) {

        if (clusterName==null)
            return ;

        File clusterDir = new File(rootDirName+seperator+clusterName);
        if (!clusterDir.exists())
        {
            clusterDir.mkdir();
        }

        if (databaseName==null)
            return;

        File databaseDir = new File(clusterDir.getAbsolutePath()+seperator+databaseName);
        if (!databaseDir.exists())
        {
            databaseDir.mkdir();
        }

        if (tableName==null)
            return;

        File tableDir = new File(databaseDir.getAbsolutePath()+seperator+tableName);
        if (!tableDir.exists())
        {
            tableDir.mkdir();
        }

    }

    private void deleteDatabaseDirs(String clusterName, String databaseName) {

        if (clusterName==null || (databaseName==null) )
            return ;

        File clusterDir = new File(rootDirName+seperator+clusterName);
        if (clusterDir.exists())
        {

            File databaseDir = new File(clusterDir.getAbsolutePath()+seperator+databaseName);
            if (databaseDir.exists())
            {
                for (File tableDir : databaseDir.listFiles())
                {
                    for (File colFile : tableDir.listFiles())
                    {
                        colFile.delete();
                    }

                    tableDir.delete();

                }

                databaseDir.delete();
            }



        }

    }

    private void deleteTableDir(String clusterName, String databaseName,String tableName) {

        if (clusterName==null || (databaseName==null || tableName==null) )
            return ;

        File clusterDir = new File(rootDirName+seperator+clusterName);
        if (clusterDir.exists())
        {

            File databaseDir = new File(clusterDir.getAbsolutePath()+seperator+databaseName);
            if (databaseDir.exists())
            {
                File tableDir =  new File(databaseDir.getAbsolutePath()+seperator+tableName);
                {
                    for (File colFile : tableDir.listFiles())
                    {
                        colFile.delete();
                    }

                    tableDir.delete();

                }

            }



        }

    }

    private  void processDatabase(File database, ClusterMetaData clusterMD, String clusterName) {

        DatabaseMetaData databaseMD = new DatabaseMetaData();
        databaseMD.setName(database.getName());

        clusterMD.addDatabaseMetaData(databaseMD);

        //  System.out.println(database.getName());
        for (File f : database.listFiles())
        {
            if (f.isDirectory())
            {
                processTable(f,databaseMD,clusterName);
            }

        }
    }

    private  void processTable(File table, DatabaseMetaData databaseMD, String clusterName) {


        DBLocks dbLocks = DBLocks.getInstance();
        try {

            dbLocks.lock(databaseMD.getName(), table.getName(), DBLocks.Type.Read);

            TableMetaData tableMetaData = null;
            try {
                FileReader reader = new FileReader(rootDirName + seperator + clusterName + seperator + databaseMD.getName() + seperator + table.getName() + seperator + table.getName() + ".meta");
                BufferedReader metaFileReader = new BufferedReader(reader);
                String s = metaFileReader.readLine();

                tableMetaData = mapper.readValue(s, TableMetaData.class);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // System.out.println(tableMetaData);


            // read meta file into
            databaseMD.addTableMetaData(tableMetaData);

        } finally {

            dbLocks.unlock(databaseMD.getName(), table.getName(), DBLocks.Type.Read);
        }

    }


}
