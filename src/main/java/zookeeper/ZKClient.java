package zookeeper;


import client.HostPortTuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes

public class ZKClient {

    // declare zookeeper instance to access ZooKeeper ensemble
    private ZooKeeper zoo;

    public ZooKeeper getZoo() {
        return zoo;
    }

    final CountDownLatch connectedSignal = new CountDownLatch(1);

    ObjectMapper mapper = new ObjectMapper();


    // Method to connect zookeeper ensemble.
    public ZooKeeper connect(String host) throws IOException, InterruptedException {

        zoo = new ZooKeeper(host, 5000, new Watcher() {

            public void process(WatchedEvent we) {

                if (we.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();

        System.out.println("Connected to zookeeper");
        return zoo;
    }

    // Method to disconnect from zookeeper server
    public void close() throws InterruptedException {
        zoo.close();
    }

    public void register(String name, HostPortTuple tuple)
    {
        try {
            String path = parentPath+"/"+name;
            if (!exists(path))
                create(path, tuple);
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    public List<HostPortTuple> get(String name)
    {
        List<HostPortTuple> hosts = new ArrayList<>();
        try {
            List<String> children = zoo.getChildren(parentPath, false);
            for(int k = 0; k < children.size(); k++) {

                byte[] b = zoo.getData(parentPath+"/"+children.get(k),false,null);
                String s = new String(b, Charsets.UTF_8);
                HostPortTuple hpt = mapper.readValue(s,HostPortTuple.class);

                hosts.add(hpt);

            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return hosts;

    }


    // Method to create znode in zookeeper ensemble
    public void create(String path, HostPortTuple tuple) throws
            Exception {

        byte[] data = mapper.writeValueAsString(tuple).getBytes(Charsets.UTF_8);
        zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }


    public boolean exists(String path) throws
            KeeperException, InterruptedException {
        return (zoo.exists(path, true)!=null );
    }


    public void update(String path, byte[] data) throws
            KeeperException, InterruptedException {
        zoo.setData(path, data, zoo.exists(path, true).getVersion());
    }


    public void delete(String path) throws KeeperException, InterruptedException {

        if (exists(path))
            ZKUtil.deleteRecursive(zoo,path);


    }


    String parentPath = "/columnDB";


}
