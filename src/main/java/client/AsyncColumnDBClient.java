package client;

import query.*;
import zookeeper.ZKClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class AsyncColumnDBClient {

   ColumnDBClient client;
    private AsyncColumnDBClient()
    {
        client = ColumnDBClient.getInstance();

    }


    static class Holder
    {
        static AsyncColumnDBClient factory = new AsyncColumnDBClient();
    }

    public static AsyncColumnDBClient getInstance()
    {
        return Holder.factory;

    }



    public Future<List<Response>> send( Request request) {
        CompletableFuture<List<Response>> future = CompletableFuture.supplyAsync(()->{return client.send(request);});
        return future;
    }


    public void send(Request request, Consumer<List<Response>> consumer) {
        CompletableFuture.supplyAsync(()->{return client.send(request);}).thenAccept(consumer);
    }

    public Future<List<Response>> send(MetaRequest request) {
        CompletableFuture<List<Response>> future = CompletableFuture.supplyAsync(()->{return client.send(request);});
        return future;
    }

    public void send(MetaRequest request, Consumer<List<Response>> consumer) {
        CompletableFuture.supplyAsync(()->{return client.send(request);}).thenAccept(consumer);

    }


    public void query(Consumer<MetaResponse> consumer) {
        CompletableFuture.supplyAsync(()->{return client.query();}).thenAccept(consumer);

    }

    public Future<MetaResponse> query() {
        CompletableFuture<MetaResponse> future = CompletableFuture.supplyAsync(()->{return client.query();});
        return future;
    }

    public void query(CountRequest request,Consumer<List<Response>> consumer) {
        CompletableFuture.supplyAsync(()->{return client.query(request);}).thenAccept(consumer);

    }


    public Future<List<Response>> query(CountRequest request) {
       CompletableFuture<List<Response>> future = CompletableFuture.supplyAsync(()->{return client.query(request);});
       return future;
    }

    public void queryData(CountRequest request,Consumer<List<DataContainer>> consumer) {
        CompletableFuture.supplyAsync(()->{return client.queryData(request);}).thenAccept(consumer);

    }


    public Future<List<DataContainer>> queryData(CountRequest request) {
       CompletableFuture<List<DataContainer>> future = CompletableFuture.supplyAsync(()->{return client.queryData(request);});
       return future;
    }



}
