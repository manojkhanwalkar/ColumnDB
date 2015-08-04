package server;

public class ServerShutDownHook implements Runnable {

    Server server;
    public ServerShutDownHook(Server s)
    {
        server=s;
    }

    public void run()
    {

        server.stop();

        server.destroy();
    }
}