package client;

public class HostPortTuple
{
    String host;
    int port;
    String name;

    public HostPortTuple(String name , String host, int port) {
        this.host = host;
        this.port = port;
        this.name = name;
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "HostPortTuple{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", name='" + name + '\'' +
                '}';
    }
}
