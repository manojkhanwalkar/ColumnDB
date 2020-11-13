package zookeeper;


import client.HostPortTuple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes

public class ZKClient {

    // declare zookeeper instance to access ZooKeeper ensemble
    private ZooKeeper zoo;
    private ZKUtil zkUtil = new ZKUtil();

    final CountDownLatch connectedSignal = new CountDownLatch(1);

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

    public void register(String name, String data)
    {
        try {
            String path = parentPath+"/"+name;
            if (!exists(path))
                create(path, data.getBytes());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void get(String name)
    {
        //TODO - get all children nodes below the parent and the data associated with them.
    }


    // Method to create znode in zookeeper ensemble
    private void create(String path, byte[] data) throws
            KeeperException, InterruptedException {
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


    @Test
    public void connectionTest() throws Exception {

        String clusterName = "cluster1";
        String clusterName1 = "cluster2";


        HostPortTuple tuple1 = new HostPortTuple("localhost",10005);
        HostPortTuple tuple2 = new HostPortTuple("localhost",10015);

        connect("localhost");

        delete(parentPath);

        create(parentPath, null);

        ObjectMapper mapper = new ObjectMapper();



        register("cluster1", mapper.writeValueAsString(tuple1));

        register("cluster2", mapper.writeValueAsString(tuple2));


        // watchNode(zoo,parentPath);


        Thread.sleep(1000);


        List<String> children = zoo.getChildren(parentPath, false);
        for(int k = 0; k < children.size(); k++) {
            System.out.print(children.get(k) + " "); //Print children's

            byte[] b = zoo.getData(parentPath+"/"+children.get(k),false,null);



            String s = new String(b, Charsets.UTF_8);

            System.out.println(s);

            HostPortTuple hpt = mapper.readValue(s,HostPortTuple.class);

            System.out.println(hpt);

        }

        System.out.println();


/*        byte[] bn = zoo.getData(path,
                false, null);
        String str = new String(bn,
                "UTF-8");
        System.out.println(str);


     */

        zoo.close();


    }




}