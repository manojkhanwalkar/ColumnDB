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

    static int numRecords=-1;

    static int size = 2;

    static int[] positions ;

    public static void main(String[] args) {

        StringBuilder data = readFile(rootDirName+seperator+clusterName+seperator+dataBaseName+seperator+tableName+seperator+"age");
       // System.out.println(data);
       // System.out.println(numRecords);

        positions = new int[numRecords];
        for (int i=0;i<positions.length;i++)
        {
            positions[i] = 1;
        }
        int count = 0;
        for (int i=0;i<data.length();i+=size)
        {
            String s = data.substring(i,i+size);
            checkCondition(s, 100, count++);
       //     System.out.println(s);
        }
        printMatchCount();

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

    private static void checkCondition(String s, int rhs , int count) {

     //   System.out.println(s);

        int lhs = Integer.parseInt(s.trim());
        if (lhs > rhs)
        {
            positions[count] = 1;
        }
        else
        {
            positions[count] = 0;
        }
    }

    private static StringBuilder readFile(String name) {

        try {
            FileReader reader = new FileReader(name);
            BufferedReader metaFileReader = new BufferedReader(reader);
            String s=  metaFileReader.readLine();
            numRecords = s.length()/size;
            metaFileReader.close();
            return new StringBuilder(s);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }


    }
}
