package rest;

import query.TableMetaData;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static rest.ColumnResource.seperator;

public class DBUtil {

    public static List<String> getDatabases(String start)
    {
        return getUnderlyingFilesorDir(start);
    }


    public static List<String> getTables(String start)
    {
        return getUnderlyingFilesorDir(start);
    }


    public static List<String> getColumns(String start)
    {
        return getUnderlyingFilesorDir(start);
    }



    private static List<String> getUnderlyingFilesorDir(String start)
    {
        File root = new File(start);

        if (root.list()!=null)
            return Arrays.asList(root.list());
        else
            return List.of();
    }


    public static void main(String[] args) {

   /*     String start = "/tmp/cluster1";

        getDatabases(start).stream().forEach(db->{
            String startdb = start+ seperator+db;
            getTables(startdb).forEach(table->{

                String starttb = startdb + seperator + table;

                var columns = getColumns(starttb);

                System.out.println(columns.contains(table+".meta"));

                TableMetaData metaData = TableMetaData.fromJSONString(starttb+seperator+table+".meta");

                metaData.getColumns().entrySet().stream().forEach(col->{

                    String name = col.getKey();

                    System.out.println(columns.contains(name));

                    int size = col.getValue().getMaxSize();

                    File file = new File(starttb+seperator+name);

                    int numRecords = (int)file.length()/size;

                    System.out.println(name + "  "  + numRecords);


                });

            });

        });*/


    }




}
