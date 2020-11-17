package client;


import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import query.*;

public class RestConnector  {

    private final int port ;
    private final String host ;

    RestTemplate restTemplate ;



    public RestConnector(String host, int port)
    {
        this.port = port ;
        this.host = host ;
     //   this.protocol = protocol;

    }



    public void connect() {


        restTemplate = new RestTemplate();

        System.out.println("Rest adapter initialized and connected");
    }

    public void finalize()
    {
        disconnect();

        System.out.println("Finalizer called ");
    }


    public void disconnect() {
        restTemplate = null;
    }

    public Response send(Request request)
    {
        HttpEntity<Request> requestEntity = new HttpEntity<>(request);
        ResponseEntity<Response> response1 = restTemplate.exchange("http://" + host + ":" + port +  "/columndb/batch", HttpMethod.POST, requestEntity, Response.class);

        return response1.getBody();
    }

    public Response send(MetaRequest request)
    {
        HttpEntity<MetaRequest> requestEntity = new HttpEntity<>(request);
        ResponseEntity<Response> response1 = restTemplate.exchange("http://" + host + ":" + port +  "/columndb/meta", HttpMethod.POST, requestEntity, Response.class);

        return response1.getBody();
    }

    public MetaResponse query(String clusterName) {
        HttpEntity<String> requestEntity = new HttpEntity<>(clusterName);
        ResponseEntity<MetaResponse> response1 = restTemplate.exchange("http://" + host + ":" + port +  "/columndb/metaquery", HttpMethod.POST, requestEntity, MetaResponse.class);

        return response1.getBody();
    }

    public Response query(CountRequest request) {
        HttpEntity<CountRequest> requestEntity = new HttpEntity<>(request);
        ResponseEntity<Response> response1 = restTemplate.exchange("http://" + host + ":" + port +  "/columndb/countquery", HttpMethod.POST, requestEntity, Response.class);

        return response1.getBody();
    }

    public DataContainer queryData(CountRequest request) {
        HttpEntity<CountRequest> requestEntity = new HttpEntity<>(request);
        ResponseEntity<DataContainer> response1 = restTemplate.exchange("http://" + host + ":" + port +  "/columndb/dataquery", HttpMethod.POST, requestEntity, DataContainer.class);

        return response1.getBody();
    }



}
