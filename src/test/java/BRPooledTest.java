import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import query.TableMetaData;
import rest.pool.BRKeyedPoolFactory;
import rest.pool.BRPoolFactory;
import rest.pool.ColumnReader;

import java.io.BufferedReader;
import java.io.IOException;

import static rest.ColumnResource.seperator;

public class BRPooledTest {


    private GenericKeyedObjectPool<String,ColumnReader> pool;
    public static void main(String[] args) throws Exception {

        BRPooledTest test = new BRPooledTest();

        test.loadColumnData();

    }

    public BRPooledTest()
    {
        pool = new GenericKeyedObjectPool<>(new BRKeyedPoolFactory());

    }

    private void loadColumnData() throws Exception
    {
       ObjectMapper mapper = new ObjectMapper();

        try {
            String name = "/home/manoj/data/ColumnDB/"+seperator+"cluster1"+seperator+"demo"+seperator+"person"+seperator+"person.meta";


            for (int i=0;i<5;i++) {

                ColumnReader reader = pool.borrowObject(name); //pool.computeIfAbsent(name,k->new GenericObjectPool<>(new BRPoolFactory(name))).borrowObject();

                BufferedReader metaFileReader = reader.getBufferedReader();

                //fis.getChannel().position(0);

                String s = metaFileReader.readLine();

                TableMetaData tableMetaData = mapper.readValue(s, TableMetaData.class);

                System.out.println(tableMetaData);


                pool.returnObject(name,reader);

            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }




}
