package rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import query.*;
import rest.pool.BRKeyedPoolFactory;
import rest.pool.ColumnReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static rest.ColumnResource.rootDirName;
import static rest.ColumnResource.seperator;

public class CountNDataProcessorForMemory {



    String clusterName ;
    String dataBaseName ;
    String tableName ;

    CountRequest request;



    public CountNDataProcessorForMemory(CountRequest request)
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
        DBLocks dbLocks = DBLocks.getInstance();
        try {

            dbLocks.lock(dataBaseName,tableName, DBLocks.Type.Read);



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

    /*private  StringBuilder readFile( String name) {

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


    }*/

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
