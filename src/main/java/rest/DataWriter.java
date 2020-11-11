package rest;

import query.ColumnMetaData;
import query.DataContainer;
import query.Request;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

    //TODO - position at end .

    //TODO - parallel write for the column files

    public void write()
    {
        DataContainer container = request.getDataContainer();
        container.getValues().entrySet().stream().forEach(ent->{

            String name = ent.getKey();
            try(BufferedWriter writer = new BufferedWriter(new FileWriter(directory+name)))
            {
                for (String s : ent.getValue()) {
                    writer.write(s);
                }

            } catch (Exception ex)
            {
                ex.printStackTrace();
            }

        });
    }

}
