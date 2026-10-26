package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;
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

        loadMetaData();
    }



    protected  StringBuilder getColumnData(String columnName)
    {
        return readFile(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+columnName);

    }







    // a column file holds all values on one line; the reader is closed on every path
      private  StringBuilder readFile( String name) {

        try (BufferedReader columnReader = new BufferedReader(new FileReader(name))) {
            String s = columnReader.readLine();
            return new StringBuilder(s == null ? "" : s);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }


    }



    ObjectMapper mapper = new ObjectMapper();



    protected void loadMetaData()
    {
        try (BufferedReader metaFileReader = new BufferedReader(new FileReader(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+tableName+".meta"))) {
            String s = metaFileReader.readLine();

            tableMetaData = mapper.readValue(s, TableMetaData.class);


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
