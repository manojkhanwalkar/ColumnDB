package query;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mkhanwalkar on 8/4/15.
 */
public class ClusterMetaData {

    String name ;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    List<DatabaseMetaData> databaseMetaData ;

    public void setDatabaseMetaData(List<DatabaseMetaData> databaseMetaData) {
        this.databaseMetaData = databaseMetaData;
    }

    public void addDatabaseMetaData(DatabaseMetaData metaData)
    {
        if (databaseMetaData==null)
            databaseMetaData = new ArrayList<>();

        databaseMetaData.add(metaData);
    }

    public List<DatabaseMetaData> getDatabaseMetaData() {
        return databaseMetaData;
    }

    @Override
    public String toString() {
        return "ClusterMetaData{" +
                "name='" + name + '\'' +
                ", databaseMetaData=" + databaseMetaData +
                '}';
    }
}
