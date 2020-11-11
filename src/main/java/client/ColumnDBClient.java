package client;

import query.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    static class HostPortTuple
    {
        String host;
        int port;

        public HostPortTuple(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    Map<String,HostPortTuple> hosts = new HashMap<>();


    public void addCluster(String name , String host, int port)
    {
        hosts.put(name,new HostPortTuple(host,port));
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
            HostPortTuple tuple = hosts.get(clusterName);
            connector = new RestConnector(tuple.host,tuple.port);
            connector.connect();
            connectors.put(clusterName,connector);

        }

        return connector;

    }


    static int BatchSize = 10; // decides how many rows will be sent in one request.



    //TODO - send in parallel to all clusters
    //TODO - break the request into chunks based on data container size and then send a chunk to one server.
    public List<Response> send( Request request) {

        RequestChunker chunker = new RequestChunker(request,BatchSize);

        final List<Response> responses = new ArrayList<>();

        hosts.keySet().parallelStream().forEach(cluster->{

            while(true) {

                var result = chunker.next();

                if (result.isPresent())
                {
                    RestConnector connector = getConnector(cluster);

                    result.get().setClusterName(cluster);

                    var res = connector.send(result.get());

                    synchronized (responses) {
                        responses.add(res);

                    }
                }
                else
                {
                    break;
                }


            }

        });

        return responses;



    }

    public List<Response> send(MetaRequest request) {

        List<Response> responses = new ArrayList<>();

        hosts.keySet().stream().forEach(cluster->{

            RestConnector connector = getConnector(cluster);

            request.getMetaData().setClusterName(cluster);

            responses.add(connector.send(request));

        });

        return responses;



    }

    public MetaResponse query() {

        String clusterName = hosts.keySet().stream().findFirst().get();
        RestConnector connector = getConnector(clusterName);
        return connector.query(clusterName);


    }

    public Response query(String clusterName , CountRequest request) {

        RestConnector connector = getConnector(clusterName);

        return connector.query(request);

    }

    public DataContainer queryData(String clusterName , CountRequest request) {

        RestConnector connector = getConnector(clusterName);

        return connector.queryData(request);

    }



}
