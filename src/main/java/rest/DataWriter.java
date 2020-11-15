package rest;

import query.ColumnMetaData;
import query.DataContainer;
import query.Request;
import query.Response;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
        ReadWriteLock lock = null;
        try {

            lock = DBLocks.getInstance().get(databaseName,tableName);

            lock.writeLock().lock();


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
        if (lock!=null)
            lock.writeLock().unlock();

    }
    }

}
