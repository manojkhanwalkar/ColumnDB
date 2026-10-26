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

import java.net.InetAddress;
import java.net.UnknownHostException;

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

        String host = configuration.getMessages().getAdvertisedHost();
        if (host == null)
            host = advertisedHostFor(factory.getBindHost());

        int port = factory.getPort();
        
        final ColumnResource resource = new ColumnResource(configuration.getMessages().getRootDir(), configuration.getMessages().getClusterName(), host,port, storageManager);
        environment.jersey().register(resource);
        environment.healthChecks().register("APIHealthCheck", new AppHealthCheck());

    }

    /**
     * The host clients are told to connect to: the bind address itself, or this machine's name when the
     * connector listens on all interfaces (0.0.0.0 can't be connected to).
     */
    static String advertisedHostFor(String bindHost) {
        if (bindHost == null || bindHost.equals("0.0.0.0") || bindHost.equals("::")) {
            try {
                return InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }
        return bindHost;
    }

}