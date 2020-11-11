package query;


public class Request
{
   String databaseName;
   String clusterName;
   String tableName;
   DataContainer dataContainer;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public DataContainer getDataContainer() {
        return dataContainer;
    }

    public void setDataContainer(DataContainer dataContainer) {
        this.dataContainer = dataContainer;
    }

    @Override
    public String toString() {
        return "Request{" +
                "databaseName='" + databaseName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dataContainer=" + dataContainer +
                '}';
    }
}
