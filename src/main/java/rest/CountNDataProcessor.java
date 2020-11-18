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

    public CountNDataProcessor(CountRequest request)
    {
        this.clusterName = request.getClusterName();
        this.dataBaseName = request.getDatabaseName();
        this.tableName = request.getTableName();

        this.request = request;

    }

     protected int[] positions ;


    public abstract DataContainer processData();

    public abstract Response processCount() ;

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
