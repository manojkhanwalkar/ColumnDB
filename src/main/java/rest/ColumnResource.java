package rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;
import server.Server;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;

@Path("/columndb")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnResource {

    public ColumnResource() {
    }


 /*

    @Path(value = "/matchdevice")
    @Produces(MediaType.APPLICATION_JSON)
    public Response matchDevice(@Context HttpServletRequest hsReq, @Valid final MatchDeviceRequest request) throws Exception {
        final Boolean enableRestEndPoint = (Boolean) ApiConfig.INSTANCE.getValue(ApiConfig.ENABLE_MATCH_DEVICE);
  */

    static final String seperator = "/";
    static final String rootDirName = "/tmp";

    static ObjectMapper mapper = new ObjectMapper();

    @Path("/countquery")
    @POST
    public Response query(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        CountProcessor processor = new CountProcessor(request);

        Response metaResponse = processor.process();

        return metaResponse;

    }


    @Path("/metaquery")
    @POST
    public MetaResponse query(@Context HttpServletRequest hsReq, @Valid String clusterName) {

        MetaResponse metaResponse = new MetaResponse();


        System.out.println(clusterName);

        File dir = new File(rootDirName+seperator+clusterName);

        ClusterMetaData clusterMD = new ClusterMetaData();
        clusterMD.setName(clusterName);

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

    private  void processTable(File table, DatabaseMetaData databaseMD, String clusterName) {

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
        } catch (IOException e) {
            e.printStackTrace();
        }

        // System.out.println(tableMetaData);


        // read meta file into
        databaseMD.addTableMetaData(tableMetaData);


    }






    @Path("/meta")
    @POST
    public Response createTable(@Context HttpServletRequest hsReq, @Valid MetaRequest request) {


       // GraphDB db = ((DBService) Server.getService("DBService")).getDatabase(request.getDbName());
        Response response=null;

        System.out.println(request);

        switch (request.getType())
        {
            case CreateTable:

                TableMetaData tableMetaData = request.getMetaData();

                String clusterName= tableMetaData.getClusterName();
                String databaseName = tableMetaData.getDatabaseName();
                String tableName = tableMetaData.getTableName();

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

                try {
                    String s = mapper.writeValueAsString(tableMetaData);

                    FileWriter writer = new FileWriter(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
                    BufferedWriter metaFileWriter = new BufferedWriter(writer);
                    metaFileWriter.write(s);
                    metaFileWriter.flush();
                    metaFileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                // response   = db.query(request);
              break ;
            default :
                response = null;

        }


        return response;
    }
}