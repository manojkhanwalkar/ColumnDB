package columndb;

import server.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DBService implements Service {


    String location;
    List<String> dbNames;

    String name ;

    String clusterName;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    int snapShotInterval = 5;

    public int getSnapShotInterval() {
        return snapShotInterval;
    }

    public void setSnapShotInterval(int snapShotInterval) {
        this.snapShotInterval = snapShotInterval;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public List<String> getDbNames() {
        return dbNames;
    }

    public void setDbNames(List<String> dbNames) {
        this.dbNames = dbNames;
    }



    @Override
    public void init() {



    }



    @Override
    public void destroy() {



    }

    @Override
    public void setName(String s) {

        name = s;

    }

    @Override
    public String getName() {
        return name;
    }
}
