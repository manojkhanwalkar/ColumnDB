import client.HostPortTuple;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import zookeeper.ZKClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

// import zookeeper classes

public class ZKEnvCreator {

    ObjectMapper mapper = new ObjectMapper();

    String parentPath = "/columnDB";


    @Test
    public void connectionTest() throws Exception {


        ZKClient client = new ZKClient();
        String clusterName = "cluster1";
        String clusterName1 = "cluster2";


        HostPortTuple tuple1 = new HostPortTuple(clusterName,"localhost",10005);
        HostPortTuple tuple2 = new HostPortTuple(clusterName1,"localhost",10015);

        client.connect("localhost");

      //  delete(parentPath);

        client.create(parentPath, null);




     /*   register("cluster1", tuple1);

        register("cluster2", tuple2);*/


        // watchNode(zoo,parentPath);


        Thread.sleep(1000);


        List<String> children = client.getZoo().getChildren(parentPath, false);
        for(int k = 0; k < children.size(); k++) {
            System.out.print(children.get(k) + " "); //Print children's

            byte[] b = client.getZoo().getData(parentPath+"/"+children.get(k),false,null);



            String s = new String(b, Charsets.UTF_8);

            System.out.println(s);

            HostPortTuple hpt = mapper.readValue(s,HostPortTuple.class);

            System.out.println(hpt);

        }

        System.out.println();




        client.close();


    }




}