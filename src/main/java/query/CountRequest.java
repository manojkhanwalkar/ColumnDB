package query;

import java.util.ArrayList;
import java.util.List;

public class CountRequest {

    String clusterName ;
    String databaseName ;
    String tableName;

    List<Criteria> criteriaList = new ArrayList<>();

    public static CountRequest duplicate(CountRequest request) {

        CountRequest countRequest = new CountRequest();
        countRequest.setDatabaseName(request.getDatabaseName());
        countRequest.setTableName(request.getTableName());
        countRequest.setCriteriaList(request.getCriteriaList());

        return countRequest;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Criteria> getCriteriaList() {
        return criteriaList;
    }

    public void setCriteriaList(List<Criteria> criteriaList) {
        this.criteriaList = criteriaList;
    }

    public void addCriteria(Criteria criteria)
    {
        criteriaList.add(criteria);
    }
}
