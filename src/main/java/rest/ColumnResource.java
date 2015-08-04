package rest;

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

    @Path("/metaquery")
    @POST
    public MetaResponse query(@Context HttpServletRequest hsReq, @Valid String request) {

        System.out.println(request);
        return null;

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