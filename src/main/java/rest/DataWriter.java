package rest;

import query.ColumnMetaData;
import query.DataContainer;
import query.Request;
import query.Response;

import java.io.*;
import java.util.concurrent.locks.ReadWriteLock;

import static rest.ColumnResource.seperator;

public class DataWriter {


    String clusterName ;
    String databaseName;
    String tableName;

    String directory ;

    Request request;

    public DataWriter(Request request, String rootDirName) {
        this.clusterName = request.getClusterName();
        this.databaseName = request.getDatabaseName();
        this.tableName = request.getTableName();

        this.request = request;

        directory = rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName+seperator;
    }



    public void write()
    {
        DBLocks dbLocks = DBLocks.getInstance();

        try {

            dbLocks.lock(databaseName,tableName, DBLocks.Type.Write);


            DataContainer container = request.getDataContainer();
        container.getValues().entrySet().parallelStream().forEach(ent->{

            String name = ent.getKey();
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(directory+name,true)))
            {
                for (String s : ent.getValue()) {
                    writer.write(s);
                }

            } catch (Exception ex)
            {
                ex.printStackTrace();
            }

        });

    } finally {
        dbLocks.unlock(databaseName,tableName, DBLocks.Type.Write);
    }
    }

}
