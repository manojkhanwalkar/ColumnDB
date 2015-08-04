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

  //  Map<String,GraphDB> databases = new HashMap<>();

    /*public Map<String, GraphDB> getDatabases() {
        return databases;
    }*/

 /*   public void setDatabases(Map<String, GraphDB> databases) {
        this.databases = databases;
    }*/

    ScheduledExecutorService scheduledPool = Executors.newScheduledThreadPool(1);


    @Override
    public void init() {

  /*      for (String name : dbNames) {

            GraphDB db = GraphDbFactory.getInstance().createDB(location,clusterName,name);

            db.restore();

            databases.put(name,db);
        }

        scheduledPool.scheduleWithFixedDelay(() -> {
            System.out.println("Hello World");
            databases.values().forEach(graphdb.GraphDB::snapshot);

        }, snapShotInterval, snapShotInterval, TimeUnit.SECONDS); */

    }

 /*   public GraphDB getDatabase(String name)
    {
        return databases.get(name);
    } */




    @Override
    public void destroy() {

 /*       try {
            scheduledPool.shutdown();
            scheduledPool.awaitTermination(1,TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (GraphDB db : databases.values())
            db.save();

*/


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
