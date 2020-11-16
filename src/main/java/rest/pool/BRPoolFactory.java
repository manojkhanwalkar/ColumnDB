package rest.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

public class BRPoolFactory extends  BasePooledObjectFactory<ColumnReader>  {

    String name;

        public BRPoolFactory()
        {

        }

        public BRPoolFactory(String name) {

          this.name = name;
        }
        @Override
        public ColumnReader create() {

            System.out.println("Object created ") ;
            return new ColumnReader(name);
        }

        /**
         * Use the default PooledObject implementation.
         */
        @Override
        public PooledObject<ColumnReader> wrap(ColumnReader buffer) {
            return new DefaultPooledObject<>(buffer);
        }

        /**
         * When an object is returned to the pool, clear the buffer.
         */
        @Override
        public void passivateObject(PooledObject<ColumnReader> pooledObject) {
            try {
                pooledObject.getObject().fis.getChannel().position(0);

                System.out.println("File stream reset");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    @Override
    public void destroyObject(PooledObject<ColumnReader> p) throws Exception {
        super.destroyObject(p);
        p.getObject().getBufferedReader().close();
    }

    @Override
    public void activateObject(PooledObject<ColumnReader> p) throws Exception {
        super.activateObject(p);
    }

    // for all other methods, the no-op implementation
        // in BasePooledObjectFactory will suffice
    }


