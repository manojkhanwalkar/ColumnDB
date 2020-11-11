package rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.Arrays;

@Path("/columndb")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnResource {


     final String rootDirName ;

    public ColumnResource(String rootDirName) {

        this.rootDirName = rootDirName;
    }



    static final String seperator = "/";

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
    public DataResponse dataquery(@Context HttpServletRequest hsReq, @Valid CountRequest request) {

        CountNDataProcessor processor = new CountNDataProcessor(request);

         DataResponse metaResponse = processor.processData();



        return metaResponse;

    }



    @Path("/metaquery")
    @POST
    public MetaResponse metaQuery(@Context HttpServletRequest hsReq, @Valid String clusterName) {

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
    public Response meta(@Context HttpServletRequest hsReq, @Valid MetaRequest request) {


        Response response=null;

        System.out.println(request);

        TableMetaData tableMetaData = request.getMetaData();

        String clusterName= tableMetaData.getClusterName();
        String databaseName = tableMetaData.getDatabaseName();
        String tableName = tableMetaData.getTableName();


        switch (request.getType())
        {
            case CreateDatabase:
                createDirs(clusterName,databaseName,null);
                return response;

            case DeleteDatabase:
                deleteDatabaseDirs(clusterName,databaseName);
                return response;
            case DeleteTable:
                deleteTableDir(clusterName,databaseName,tableName);
                return response;
            case DeleteColumn: {

                String metaFile = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta";

                TableMetaData combined = TableMetaData.removeColumns(TableMetaData.fromJSONString(metaFile),tableMetaData);

                try {
                    String s = mapper.writeValueAsString(combined);

                    FileWriter writer = new FileWriter(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
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
            }


            case AddColumn: {
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

                String metaFile = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta";

                TableMetaData combined = TableMetaData.addColumns(TableMetaData.fromJSONString(metaFile),tableMetaData);

                try {
                    String s = mapper.writeValueAsString(combined);

                    FileWriter writer = new FileWriter(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator+tableName+".meta");
                    BufferedWriter metaFileWriter = new BufferedWriter(writer);
                    metaFileWriter.write(s);
                    metaFileWriter.flush();
                    metaFileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                return response;
            }
            case CreateTable:

                createDirs(clusterName,databaseName,tableName);

                File dir = new File(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName);
                //dir.mkdirs();

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