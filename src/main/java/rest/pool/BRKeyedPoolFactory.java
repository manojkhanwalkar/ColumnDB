package rest.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

public class BRKeyedPoolFactory implements KeyedPooledObjectFactory<String,ColumnReader> {

    @Override
    public PooledObject<ColumnReader> makeObject(String s) throws Exception {
       // System.out.println("Called make object");
        return new DefaultPooledObject<>(new ColumnReader(s));
    }

    @Override
    public void destroyObject(String s, PooledObject<ColumnReader> pooledObject) throws Exception {
       // System.out.println("Called destroy object");

        pooledObject.getObject().getBufferedReader().close();

    }

    @Override
    public boolean validateObject(String s, PooledObject<ColumnReader> pooledObject) {
        //System.out.println("Called validate object");

        return true;
    }

    @Override
    public void activateObject(String s, PooledObject<ColumnReader> pooledObject) throws Exception {

        //System.out.println("Called activate object");


    }

    @Override
    public void passivateObject(String s, PooledObject<ColumnReader> pooledObject) throws Exception {

        //System.out.println("Called passivate object");

        try {
            pooledObject.getObject().fis.getChannel().position(0);

            //System.out.println("File stream reset");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}


