package rest;

import io.dropwizard.Application;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.server.HttpConnectionFactory;
import storage.FileStorageManager;
import storage.MemoryStorageManager;
import storage.StorageManager;

public class ColumnApplication extends Application<ExampleServiceConfiguration> {

    StorageManager storageManager;

    public ColumnApplication(StorageManager storageManager)
    {
        this.storageManager = storageManager;
    }

    @Override
    public String getName() {
        return "hello-world";
    }

    @Override
    public void initialize(Bootstrap<ExampleServiceConfiguration> bootstrap) {
        // nothing to do yet


    }

    //TODO - get storage manager impl from configuration or a factory
    @Override
    public void run(ExampleServiceConfiguration configuration,
                    Environment environment) {
        // nothing to do yet

        var factory = ((HttpConnectorFactory) ((DefaultServerFactory)configuration.getServerFactory()).getApplicationConnectors().get(0));

        String host = factory.getBindHost();
        if (host==null)
            host="localhost";

        int port = factory.getPort();
        
        final ColumnResource resource = new ColumnResource(configuration.getMessages().getRootDir(), configuration.getMessages().getClusterName(), host,port, storageManager);
        environment.jersey().register(resource);
        environment.healthChecks().register("APIHealthCheck", new AppHealthCheck());

    }

}