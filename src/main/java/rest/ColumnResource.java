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

        CountNDataProcessor processor = new CountNDataProcessor(request);

        Response metaResponse = processor.processCount();



        return metaResponse;

    }

    @Path("/dataquery")
    @POST
    public DataContainer dataquery(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        CountNDataProcessor processor = new CountNDataProcessor(request);

         DataContainer metaResponse = processor.processData();



        return metaResponse;

    }


    @Path("/batch")
    @POST
    public Response batch(@Valid Request request) {


        storageManager.write(request);

        return null;
    }




    @Path("/metaquery")
    @POST
    public MetaResponse metaQuery(@Context HttpServletRequest hsReq, @Valid String clusterName) {


        MetaResponse metaResponse = new MetaResponse();

        ClusterMetaData cmd = new ClusterMetaData();
        cmd.setName(clusterName);
        storageManager.populateClusterMetaData(cmd);

        metaResponse.setClusterMetaData(cmd);


        return metaResponse;


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

                if (!storageManager.existsDatabase(databaseName)) {
                    dbLocks.createLock(databaseName);

                    storageManager.createDatabase(databaseName);


                }
                return response;

            case DeleteDatabase:
                storageManager.deleteDatabase(databaseName);
                dbLocks.deleteLock(databaseName);
                return response;
            case DeleteTable:

                storageManager.deleteTable(databaseName,tableName);
                dbLocks.deleteLock(databaseName,tableName);
                return response;
            case DeleteColumn: {


                try {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);

                    storageManager.deleteColumn(databaseName,tableName,tableMetaData); //TODO - combined logic to be moved into storage manager

                    return response;

                } finally {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);
                }
            }


            case AddColumn: {


                try {
                    dbLocks.lock(databaseName, tableName, DBLocks.Type.Write);

                    storageManager.addColumn(databaseName,tableName,tableMetaData);

                    return response;

                } finally {
                    dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);
                }
            }
            case CreateTable:

                if (storageManager.existsDatabase(databaseName) && !storageManager.existsTable(databaseName,tableName))
                {
                    dbLocks.createLock(databaseName, tableName);

                    dbLocks.lock(databaseName,tableName, DBLocks.Type.Write);
                    storageManager.createTable(databaseName,tableName, tableMetaData);
                    dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);
                }
              break ;
            default :
                response = null;

        }


        return response;
    }




    //


}