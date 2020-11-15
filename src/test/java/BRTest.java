import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import query.TableMetaData;
import rest.pool.BRPoolFactory;
import rest.pool.ColumnReader;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static rest.ColumnResource.seperator;

public class BRTest {


    public static void main(String[] args) throws Exception {

        BRTest test = new BRTest();

        test.loadMetaData();

    }

    private Map<String,ObjectPool<ColumnReader>> pool = new HashMap<>();
//ReaderUtil readerUtil = new ReaderUtil(new GenericObjectPool<StringBuffer>(new StringBufferFactory()));

    private void loadMetaData() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        try {
            String name = "/home/manoj/data/ColumnDB/"+seperator+"cluster1"+seperator+"demo"+seperator+"person"+seperator+"person"+".meta";

            //FileInputStream fis = new FileInputStream();
            //FileReader reader = new FileReader("/home/manoj/data/ColumnDB/"+seperator+"cluster1"+seperator+"demo"+seperator+"person"+seperator+"person"+".meta");
            //BufferedReader metaFileReader = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

            for (int i=0;i<5;i++) {

                ColumnReader reader = pool.computeIfAbsent(name,k->new GenericObjectPool<>(new BRPoolFactory(name))).borrowObject();

                BufferedReader metaFileReader = reader.getBufferedReader();

                //fis.getChannel().position(0);

                String s = metaFileReader.readLine();

                TableMetaData tableMetaData = mapper.readValue(s, TableMetaData.class);

                System.out.println(tableMetaData);


                pool.get(name).returnObject(reader);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }



}
