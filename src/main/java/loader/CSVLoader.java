package loader;

import java.io.File;

public class CSVLoader {

    String rootDirName ;
    String clusterName;
    String databaseName ;
    String tableName ;

    static final String seperator = "/";

    public CSVLoader(String rootDirName, String clusterName, String databaseName, String tableName, String fileName) {
        this.rootDirName = rootDirName;
        this.clusterName = clusterName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static void main(String[] args) {

        CSVLoader loader = new CSVLoader("/tmp","cluster1","demo","person","/tmp/person.csv");

        loader.load();

    }

    private void load() {

        File dir = new File(rootDirName+seperator+clusterName+seperator+databaseName+seperator+tableName);
        dir.mkdirs();
    }


}
