package client;

import query.DataContainer;
import query.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RequestChunker {

    AtomicInteger index= new AtomicInteger(0);

    Request request;

    List<DataContainer> chunks ;

    public RequestChunker(Request request, int size)
    {
        this.request = request;

        breakIntoChunks(size);
    }


    private void breakIntoChunks(int size)
    {
        chunks = new ArrayList<>();

        DataContainer dataContainer = request.getDataContainer();

        int max = dataContainer.getValues().entrySet().stream().findFirst().map(e->e.getValue()).get().size();


        int start=0;
        while(start<max)
        {
            int end =  Math.min(max, start+size);
            DataContainer tmp = new DataContainer();
            final int s = start;
            final int e = end;
            dataContainer.getValues().entrySet().forEach(entry->{

                String col = entry.getKey();

                List<String> values = new ArrayList<>();
                for (int i=s;i<e;i++)
                {
                    values.add(entry.getValue().get(i));
                }

                tmp.addValues(col,values);
            });

            chunks.add(tmp);
            start = end+1;

        }

    }



    public  Optional<Request> next()
    {
        int i = index.getAndIncrement();
        if (i>=chunks.size())
            return Optional.empty();


        Request r = new Request();

        r.setTableName(request.getTableName());
        r.setClusterName(request.getClusterName());
        r.setDatabaseName(request.getDatabaseName());

        r.setDataContainer(chunks.get(i));

        return Optional.of(r);
    }
}
