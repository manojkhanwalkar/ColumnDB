package rest;

import client.HostPortTuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;
import storage.StorageManager;
import zookeeper.ZKClient;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;

@Path("/columndb")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnResource {


    public static  String rootDirName ;
    public static  String clusterName ;
    public  static final String seperator = "/";

    public static final String zkParentPath = "/columnDB";

    ZKClient zkClient = new ZKClient();

    DBLocks locks ;



    StorageManager storageManager ;

    public ColumnResource(String rootDirName, String clusterName, String host, int port, StorageManager storageManager) {

        this.rootDirName = rootDirName;
        this.clusterName = clusterName;
        this.storageManager = storageManager;

        storageManager.setRootDirName(rootDirName);
        storageManager.setClusterName(clusterName);


        try {
            zkClient.connect("localhost");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        zkClient.register(clusterName, new HostPortTuple(clusterName,host,port));

        locks = DBLocks.getInstance();

        locks.createLocks(rootDirName+"/"+clusterName);


    }




    static ObjectMapper mapper = new ObjectMapper();

    @Path("/countquery")
    @POST
    public Response query(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        CountNDataProcessor processor = new CountNDataProcessor(request,storageManager);

        Response metaResponse = processor.processCount();



        return metaResponse;

    }

    @Path("/dataquery")
    @POST
    public DataContainer dataquery(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        CountNDataProcessor processor = new CountNDataProcessor(request, storageManager);

         DataContainer metaResponse = processor.processData();



        return metaResponse;

    }


    @Path("/batch")
    @POST
    public Response batch(@Valid Request request) {



        storageManager.write(request);
        DataWriter writer = new DataWriter(request,rootDirName);

        writer.write();

        // get the cluster + database + tablename from container and navigate to that directory.
        // for each column in the map , open the corresponding coclumn file and go to the end
        // append the list of values to the file



        return null;
    }




    @Path("/metaquery")
    @POST
    public MetaResponse metaQuery(@Context HttpServletRequest hsReq, @Valid String clusterName) {



        MetaResponse metaResponse = new MetaResponse();

        File dir = new File(rootDirName+seperator+clusterName);

        ClusterMetaData clusterMD = new ClusterMetaData();
        clusterMD.setName(clusterName);

        storageManager.populateClusterMetaData(clusterMD);

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


        metaResponse.setClusterMetaData(clusterMD);


        return metaResponse;


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





/* synchronized added here to prevent two meta changes from happening at the same time */

    @Path("/meta")
    @POST
    public synchronized Response meta(@Context HttpServletRequest hsReq, @Valid MetaRequest request) {


        Response response=null;

        System.out.println(request);

        TableMetaData tableMetaData = request.getMetaData();

        String clusterName= tableMetaData.getClusterName();
        String databaseName = tableMetaData.getDatabaseName();
        String tableName = tableMetaData.getTableName();

        var dbLocks = DBLocks.getInstance();

        switch (request.getType())
        {

            case CreateDatabase:

               String databasePath = rootDirName+seperator+clusterName+seperator+databaseName;
               if (!exists(databasePath)) {
                   dbLocks.createLock(databaseName);
                   createDirs(clusterName, databaseName, null);
               }

                if (!storageManager.existsDatabase(databaseName)) {
                    dbLocks.createLock(databaseName);

                    storageManager.createDatabase(databaseName);


                }
                return response;

            case DeleteDatabase:

                deleteDatabaseDirs(clusterName,databaseName);
                storageManager.deleteDatabase(databaseName);
                dbLocks.deleteLock(databaseName);
                return response;
            case DeleteTable:
                deleteTableDir(clusterName,databaseName,tableName);
                storageManager.deleteTable(databaseName,tableName);
                dbLocks.deleteLock(databaseName,tableName);
                return response;
            case DeleteColumn: {


                try {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);
                    String metaFile = rootDirName + seperator + clusterName + seperator + databaseName + seperator + tableName + seperator + tableName + ".meta";

                    storageManager.deleteColumn(databaseName,tableName,tableMetaData); //TODO - combined logic to be moved into storage manager

                    TableMetaData combined = TableMetaData.removeColumns(TableMetaData.fromJSONString(metaFile), tableMetaData);

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
                    tableMetaData.getColumns().values().stream().forEach(cmd -> {


                        File file = new File(dir + seperator + cmd.getColumnName());
                        file.delete();
                    });

                    return response;

                } finally {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);
                }
            }


            case AddColumn: {


                try {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);

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

                    storageManager.addColumn(databaseName,tableName,tableMetaData); //TODO - combined logic to be moved into storage manager


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


                    return response;

                } finally {
                    dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);
                }
            }
            case CreateTable:
                String tablePath = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName;
                if (!exists(tablePath)) {

                    dbLocks.createLock(databaseName, tableName);

                    dbLocks.lock(databaseName,tableName, DBLocks.Type.Write);

                    storageManager.createTable(databaseName,tableName);


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

                    dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);

                }

                // response   = db.query(request);
              break ;
            default :
                response = null;

        }


        return response;
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


    private boolean exists(String dir)
    {
        File dirFile = new File(dir);
        if (dirFile.exists())
            return true;
        else
            return false;
    }

    //

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