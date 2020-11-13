package client;

public class HostPortTuple
{
    String host;
    int port;

    public HostPortTuple(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public HostPortTuple() {
    }

    @Override
    public String toString() {
        return "HostPortTuple{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
