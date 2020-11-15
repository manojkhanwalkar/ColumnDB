package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import query.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static rest.ColumnResource.rootDirName;
import static rest.ColumnResource.seperator;

public class CountNDataProcessor {

   // static final String seperator = "/";
    //static final String rootDirName = "/tmp";  //TODO - pick from config


    String clusterName ;
    String dataBaseName ;
    String tableName ;

   //  int numRecords=100; //TODO - this value needs to be updated in the metadata .

    CountRequest request;

    // get metadata for max size of each column .

    // for now number of records is hardcoded .

    public CountNDataProcessor(CountRequest request)
    {
        this.clusterName = request.getClusterName();
        this.dataBaseName = request.getDatabaseName();
        this.tableName = request.getTableName();

        this.request = request;

        loadMetaData();
    }

     int[] positions ;


    public DataContainer processData()
    {
        ReadWriteLock lock = null;
        try {

            lock = DBLocks.getInstance().get(dataBaseName,tableName);

            lock.readLock().lock();


            DataContainer response = new DataContainer();
            processCount();  // gets the positions .
            // for each column , read file , check data against positions and create a list of column results .

            tableMetaData.getColumns().values().stream().forEach(col -> {

                StringBuilder data = readFile(rootDirName + seperator + clusterName + seperator + dataBaseName + seperator + tableName + seperator + col.getColumnName());
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
            if (lock!=null)
                lock.readLock().unlock();

        }
    }


    public Response processCount() {

        ReadWriteLock lock = null;
        try {

            lock = DBLocks.getInstance().get(dataBaseName,tableName);

            lock.readLock().lock();

            Response response = new Response();


        request.getCriteriaList().stream().forEach(criteria->{

            int size = tableMetaData.getColumnMetaData(criteria.getColumnName()).getMaxSize();
            checkCondition(size,criteria.getColumnName(), criteria.getRhs(), criteria.getType());

        });


        int count = getMatchCount();


        response.setResult("Count matching the condition is " + count);
        return response ;

        } finally {
            if (lock!=null)
                lock.readLock().unlock();

        }
    }


    private  void checkCondition(int size , String columnName, String rhs, ConditionType type)
    {
        StringBuilder data = readFile(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+columnName);

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



    private  int getMatchCount() {
        int count = 0;

        for (int i=0;i<positions.length;i++)
        {
            if (positions[i]==1)
                count++;
        }

        return count;


    }

    private  void checkCondition(String s, String s1 , ConditionType type,  int count) {

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

    private  StringBuilder readFile( String name) {

        try {
            FileReader reader = new FileReader(name);
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s=  metaFileReader.readLine();
            metaFileReader.close();

            return new StringBuilder(s);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }


    }


    ObjectMapper mapper = new ObjectMapper();
    TableMetaData tableMetaData=null;


    private void loadMetaData()
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
