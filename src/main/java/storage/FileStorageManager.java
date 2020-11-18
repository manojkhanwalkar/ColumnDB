package storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import query.ClusterMetaData;
import query.Request;
import query.TableMetaData;
import rest.DBLocks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

    }

    @Override
    public void populateClusterMetaData(ClusterMetaData clusterMD) {

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

}
