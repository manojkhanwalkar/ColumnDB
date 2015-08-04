package query;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mkhanwalkar on 8/4/15.
 */
public class DatabaseMetaData {

    String name ;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    List<TableMetaData>  tableMetaData ;

    public List<TableMetaData> getTableMetaData() {
        return tableMetaData;
    }

    public void setTableMetaData(List<TableMetaData> tableMetaData) {
        this.tableMetaData = tableMetaData;
    }

    public void addTableMetaData(TableMetaData metadata)
    {
        if (tableMetaData==null)
        {
            tableMetaData = new ArrayList<>();
        }

        tableMetaData.add(metadata);

    }
}
