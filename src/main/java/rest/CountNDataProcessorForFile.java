package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import query.*;
import rest.pool.BRKeyedPoolFactory;
import rest.pool.ColumnReader;
import storage.StorageManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static rest.ColumnResource.rootDirName;
import static rest.ColumnResource.seperator;

public class CountNDataProcessorForFile extends CountNDataProcessor {



    public CountNDataProcessorForFile(CountRequest request,StorageManager storageManager)
    {
        super(request,storageManager);
        this.request = request;
        pool = new GenericKeyedObjectPool<>(new BRKeyedPoolFactory());

        loadMetaData();
    }



    protected  StringBuilder getColumnData(String columnName)
    {
        return readFile(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+columnName);

    }







    private GenericKeyedObjectPool<String, ColumnReader> pool;
      private  StringBuilder readFile( String name) {

        try {
 //           FileReader reader = new FileReader(name);
   //         BufferedReader metaFileReader = new BufferedReader(reader);

            ColumnReader reader = pool.borrowObject(name); //pool.computeIfAbsent(name,k->new GenericObjectPool<>(new BRPoolFactory(name))).borrowObject();

            BufferedReader metaFileReader = reader.getBufferedReader();


            String s=  metaFileReader.readLine();
          //  metaFileReader.close();

            pool.returnObject(name,reader);

            return new StringBuilder(s);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }



    ObjectMapper mapper = new ObjectMapper();



    protected void loadMetaData()
    {
        try {
            FileReader reader = new FileReader(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+tableName+".meta");
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s = metaFileReader.readLine();

            tableMetaData = mapper.readValue(s, TableMetaData.class);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
