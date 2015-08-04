package client;

import query.MetaRequest;
import query.MetaResponse;
import query.Request;
import query.Response;

import java.util.HashMap;
import java.util.Map;

public class ColumnDBClient {

    ThreadLocal<Map<String,RestConnector>> localConnector = new ThreadLocal<>();

    private ColumnDBClient()
    {

    }


    static class Holder
    {
        static ColumnDBClient factory = new ColumnDBClient();
    }

    public static ColumnDBClient getInstance()
    {
        return Holder.factory;

    }

    Map<String,Integer> ports = new HashMap<>();
    Map<String,String> hosts = new HashMap<>();

    public void addCluster(String name , int port)
    {
        ports.put(name,port);
    }

    public void addCluster(String name , String host)
    {
        hosts.put(name,host);
    }



    private RestConnector getConnector(String clusterName) {

        Map<String,RestConnector> connectors = localConnector.get();
        if (connectors==null)
        {
            connectors = new HashMap<>();
            localConnector.set(connectors);
        }

        RestConnector connector = connectors.get(clusterName);
        if (connector==null)
        {
            connector = new RestConnector(hosts.get(clusterName), ports.get(clusterName));
            connector.connect();
            connectors.put(clusterName,connector);

        }

        return connector;

    }





    public Response send(String clusterName , Request request) {

        RestConnector connector = getConnector(clusterName);

        return connector.send(request);

    }

    public Response send(String clusterName , MetaRequest request) {

        RestConnector connector = getConnector(clusterName);

        return connector.send(request);

    }

    public MetaResponse query(String clusterName) {

        RestConnector connector = getConnector(clusterName);
        return connector.query(clusterName);


    }




}
