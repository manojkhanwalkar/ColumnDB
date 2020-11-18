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

public abstract class CountNDataProcessor {

   // static final String seperator = "/";
    //static final String rootDirName = "/tmp";  //TODO - pick from config


    protected String clusterName ;
    protected String dataBaseName ;
    protected String tableName ;

   //  int numRecords=100; //TODO - this value needs to be updated in the metadata .

    CountRequest request;

    // get metadata for max size of each column .

    // for now number of records is hardcoded .

    StorageManager storageManager;

    TableMetaData tableMetaData=null;

    public CountNDataProcessor(CountRequest request,StorageManager storageManager)
    {
        this.clusterName = request.getClusterName();
        this.dataBaseName = request.getDatabaseName();
        this.tableName = request.getTableName();

        this.request = request;

        this.storageManager = storageManager;

    }

     protected int[] positions ;


    public DataContainer processData()
    {
        DBLocks dbLocks = DBLocks.getInstance();
        try {

            dbLocks.lock(dataBaseName,tableName, DBLocks.Type.Read);



            DataContainer response = new DataContainer();
            processCount();  // gets the positions .
            // for each column , read file , check data against positions and create a list of column results .

            tableMetaData.getColumns().values().stream().forEach(col -> {

                StringBuilder data = getColumnData(col.getColumnName()) ;

                int size = col.getMaxSize();
                List<String> colResults = new ArrayList<String>();
                for (int i = 0; i < positions.length; i++) {
                    if (positions[i] == 0)
                        continue;

                    int start = i * size;
                    String s = data.substring(start, start + size);

                    colResults.add(s);

                }

                response.addValues(col.getColumnName(), colResults);


            });

            return response;

        } finally {
            dbLocks.unlock(dataBaseName,tableName, DBLocks.Type.Read);

        }
    }


    public Response processCount() {

        DBLocks dbLocks = DBLocks.getInstance();
        try {

            dbLocks.lock(dataBaseName,tableName, DBLocks.Type.Read);

            Response response = new Response();


            request.getCriteriaList().stream().forEach(criteria->{

                int size = tableMetaData.getColumnMetaData(criteria.getColumnName()).getMaxSize();
                checkCondition(size,criteria.getColumnName(), criteria.getRhs(), criteria.getType());

            });


            int count = getMatchCount();


            response.setResult("Count matching the condition is " + count);
            return response ;

        } finally {
            dbLocks.unlock(dataBaseName,tableName, DBLocks.Type.Read);

        }
    }


    protected abstract StringBuilder getColumnData(String columnName);


    protected  void checkCondition(int size , String columnName, String rhs, ConditionType type)
    {
        StringBuilder data = getColumnData(columnName);


        if (positions==null)
        {

            positions = new int[data.length()/size];
            for (int i=0;i<positions.length;i++)
            {
                positions[i] = 1;
            }


        }

        int count = 0;
        for (int i=0;i<data.length();i+=size)
        {
            String s = data.substring(i,i+size);
            checkCondition(s, rhs, type , count++);
            //     System.out.println(s);
        }

    }



    protected  int getMatchCount() {
        int count = 0;

        for (int i=0;i<positions.length;i++)
        {
            if (positions[i]==1)
                count++;
        }

        return count;


    }

    protected  void checkCondition(String s, String s1 , ConditionType type,  int count) {

        //   System.out.println(s);
        if (positions[count]==0)
            return ;

        switch (type)
        {
            case GT :
            {
                int lhs = Integer.parseInt(s.trim());
                int rhs = Integer.parseInt(s1.trim());
                if (lhs > rhs)
                {
                    // positions[count] = 1;
                }
                else
                {
                    positions[count] = 0;
                }
                break;

            }
            case LT :
            {
                int lhs = Integer.parseInt(s.trim());
                int rhs = Integer.parseInt(s1.trim());
                if (lhs < rhs)
                {
                    //  positions[count] = 1;
                }
                else
                {
                    positions[count] = 0;
                }
                break;

            }
            case EQ :
            {
                if (s.trim().equals(s1))
                {
                    //  positions[count] = 1;
                }
                else
                {
                    positions[count] = 0;
                }
                break;

            }
            case IN :
            {
                String [] sA = s1.split(",");
                s = s.trim();
                for (int i=0;i<sA.length;i++)
                {
                    if (s.equals(sA[i]))
                        return ;

                }

                positions[count] = 0;
                break;

            }



        }

    }





    protected abstract void loadMetaData();


}
