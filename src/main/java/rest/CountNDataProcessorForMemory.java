package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import query.*;
import rest.pool.BRKeyedPoolFactory;
import rest.pool.ColumnReader;
import storage.MemoryStorageManager;
import storage.StorageManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static rest.ColumnResource.rootDirName;
import static rest.ColumnResource.seperator;

public class CountNDataProcessorForMemory extends CountNDataProcessor {




    public CountNDataProcessorForMemory(CountRequest request, StorageManager storageManager)
    {
        super(request,storageManager);

        loadMetaData();
    }







    protected StringBuilder getColumnData(String columnName)
    {
        MemoryStorageManager memoryStorageManager = (MemoryStorageManager)storageManager;
        return memoryStorageManager.getColumnData(dataBaseName,tableName,columnName);
    }






    protected void loadMetaData()
    {
        MemoryStorageManager memoryStorageManager = (MemoryStorageManager)storageManager;

        tableMetaData = memoryStorageManager.getMetaData(dataBaseName,tableName);
    }

}
