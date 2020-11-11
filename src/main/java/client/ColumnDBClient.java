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
        String clusterName = "cluster1";
        String clusterName1 = "cluster2";


        addCluster(clusterName,"localhost",10005);
        addCluster(clusterName1,"localhost",10015);


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


    private void addCluster(String name , String host, int port)
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

        final List<Response> responses = new ArrayList<>();

        hosts.keySet().parallelStream().forEach(cluster->{

            RestConnector connector = getConnector(cluster);

            request.getMetaData().setClusterName(cluster);

            Response response = connector.send(request);

            synchronized (responses) {
                responses.add(response);
            }

        });

        return responses;



    }

    public MetaResponse query() {

        String clusterName = hosts.keySet().stream().findFirst().get();
        RestConnector connector = getConnector(clusterName);
        return connector.query(clusterName);


    }

    public List<Response> query(CountRequest request) {

        final List<Response> responses = new ArrayList<>();
        hosts.keySet().parallelStream().forEach(cluster->{

            CountRequest countRequest = CountRequest.duplicate(request);

            countRequest.setClusterName(cluster);

            RestConnector connector = getConnector(cluster);

            Response response = connector.query(countRequest);

            synchronized (responses)
            {
                responses.add(response);
            }


        });

       return responses;

    }

    public List<DataContainer> queryData(CountRequest request) {

        final List<DataContainer> responses = new ArrayList<>();
        hosts.keySet().parallelStream().forEach(cluster->{

            CountRequest countRequest = CountRequest.duplicate(request);

            countRequest.setClusterName(cluster);

            RestConnector connector = getConnector(cluster);

            DataContainer response = connector.queryData(request);

            synchronized (responses)
            {
                responses.add(response);
            }


        });

        return responses;



    }



}
