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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.Map;

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




    @Path("/countquery")
    @POST
    public Response query(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        validate(request);
        Response metaResponse  = storageManager.processCount(request);





        return metaResponse;

    }

    @Path("/dataquery")
    @POST
    public DataContainer dataquery(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        validate(request);
        DataContainer metaResponse  = storageManager.processData(request);




        return metaResponse;

    }


    @Path("/batch")
    @POST
    public Response batch(@Valid Request request) {

        validate(request);

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

        validate(request);

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
                    dbLocks.unlock(databaseName, tableName, DBLocks.Type.Write);
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




    /*
     * Input validation. Cluster, database, table and column names are used to build file paths
     * under rootDirName, so they must be plain identifiers (no "/", "..", or file extensions),
     * and a request may only address the cluster this server was configured for.
     */

    private static WebApplicationException badRequest() {
        return new WebApplicationException(javax.ws.rs.core.Response.Status.BAD_REQUEST);
    }

    private static void checkName(String name) {
        if (!NameValidator.isValid(name))
            throw badRequest();
    }

    private static void checkCluster(String requestedCluster) {
        if (requestedCluster == null || !requestedCluster.equals(clusterName))
            throw badRequest();
    }

    private static void checkColumns(Map<String, ?> columns) {
        if (columns == null)
            throw badRequest();
        columns.keySet().forEach(ColumnResource::checkName);
    }

    static void validate(CountRequest request) {
        if (request == null || request.getCriteriaList() == null)
            throw badRequest();
        checkCluster(request.getClusterName());
        checkName(request.getDatabaseName());
        checkName(request.getTableName());
        for (Criteria criteria : request.getCriteriaList()) {
            if (criteria == null || criteria.getType() == null || criteria.getRhs() == null)
                throw badRequest();
            checkName(criteria.getColumnName());
        }
    }

    static void validate(Request request) {
        if (request == null || request.getDataContainer() == null)
            throw badRequest();
        checkCluster(request.getClusterName());
        checkName(request.getDatabaseName());
        checkName(request.getTableName());
        checkColumns(request.getDataContainer().getValues());
    }

    static void validate(MetaRequest request) {
        if (request == null || request.getType() == null || request.getMetaData() == null)
            throw badRequest();
        TableMetaData metaData = request.getMetaData();
        checkName(metaData.getDatabaseName());
        switch (request.getType()) {
            case CreateDatabase:
            case DeleteDatabase:
                return;
            case DeleteTable:
                checkName(metaData.getTableName());
                return;
            default:
                checkName(metaData.getTableName());
                checkColumns(metaData.getColumns());
                metaData.getColumns().values().forEach(cmd -> {
                    if (cmd == null)
                        throw badRequest();
                    checkName(cmd.getColumnName());
                });
        }
    }

}