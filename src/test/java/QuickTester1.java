import query.ConditionType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class QuickTester1 {

    static final String seperator = "/";
    static final String rootDirName = "/tmp";
    static final String clusterName = "cluster1";
    static final String dataBaseName = "demo";
    static final String tableName = "person";

    static int numRecords=100; //TODO - this value needs to be updated in the metadata .

    static int size = 2;

    static int[] positions ;

    public static void main(String[] args) {

        positions = new int[numRecords];
        for (int i=0;i<positions.length;i++)
        {
            positions[i] = 1;
        }
        checkCondition("age", "25", ConditionType.GT);
        size = 7;
        checkCondition("income", "700000", ConditionType.LT);
        size = 1;
        checkCondition("gender", "F", ConditionType.EQ);
        printMatchCount();

    }

    private static void checkCondition(String columnName, String rhs, ConditionType type)
    {
        StringBuilder data = readFile(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+columnName);

        int count = 0;
        for (int i=0;i<data.length();i+=size)
        {
            String s = data.substring(i,i+size);
            checkCondition(s, rhs, type , count++);
            //     System.out.println(s);
        }

    }



    private static void printMatchCount() {
        int count = 0;

        for (int i=0;i<positions.length;i++)
        {
            if (positions[i]==1)
                count++;
        }

        System.out.println("Match number is " + count);


    }

    private static void checkCondition(String s, String s1 , ConditionType type,  int count) {

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
                    positions[count] = 1;
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
                    positions[count] = 1;
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
                    positions[count] = 1;
                }
                else
                {
                    positions[count] = 0;
                }
                break;

            }



        }

    }

    private static StringBuilder readFile(String name) {

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
}
