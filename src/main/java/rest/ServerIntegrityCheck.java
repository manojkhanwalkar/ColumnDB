package rest;

import query.TableMetaData;

import java.io.File;

import static rest.ColumnResource.seperator;

import static rest.DBUtil.*;

public class ServerIntegrityCheck {


    boolean check=true;
    public boolean integrityCheck()
    {
        return check;
    }


    String errorMessage = "";
    public String errorMessage() {

        return errorMessage;

    }


    public ServerIntegrityCheck(String start)
    {
        // for every table directory - read the meta file and then check if all columns exist as per the meta file .
        // for each column check if the number of records are the same for all columns in a table.

        //String start = "/tmp/cluster1";

        var databases = getDatabases(start);
        if (databases.isEmpty())
        {

            return;
        }

        for (String db : databases)
        {
            String startdb = start+ seperator+db;
            var tables = getTables(startdb);

            int numRecords = -1;
            for (String table : tables )
            {
                String starttb = startdb + seperator + table;

                var columns = getColumns(starttb);
                if (!columns.contains(table+".meta")) {
                    errorMessage = "Meta data file not found";
                    check = false;
                    return;
                }
                TableMetaData metaData = TableMetaData.fromJSONString(starttb+seperator+table+".meta");

                var entrySetIter = metaData.getColumns().entrySet().iterator();
                while(entrySetIter.hasNext())
                {
                    var entrySet = entrySetIter.next();
                    String name = entrySet.getKey();

                    int size = entrySet.getValue().getMaxSize();

                    File file = new File(starttb+seperator+name);

                     if (numRecords==-1) {
                         numRecords = (int) file.length() / size;
                     }
                     else
                     {
                         if (numRecords!=(int) file.length() / size)
                         {
                             check = false;
                             errorMessage = "Number of records do not match in table " + table + " for column " + name;
                             return;
                         }
                     }


                }


            }


        }


    }



}
