package server;


import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ServiceList {

    Map<String, Service> services = new LinkedHashMap<String, Service>();

    public Collection<Service> getServices() {
        return services.values();
    }



    List<Service> listServices;
    public void setListServices(List<Service> listServices) {

        for (Service service : listServices)
        {
            services.put(service.getName(),service);
        }
    }

    public List<Service> getListServices() {

        return listServices;
    }

    public Service getService(String name)
    {
        return services.get(name);
    }
}