package rest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import query.*;
import storage.FileStorageManager;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

/** Batches and queries may only touch columns the table defines. */
public class UnknownColumnTest {

    private File root;
    private FileStorageManager storage;

    @Before
    public void setUp() throws Exception {
        root = Files.createTempDirectory("columndb-test").toFile();
        ColumnResource.rootDirName = root.getPath();
        ColumnResource.clusterName = "cluster1";
        new File(root, "cluster1").mkdirs();
        storage = new FileStorageManager();
        storage.setRootDirName(root.getPath());
        storage.setClusterName("cluster1");

        DBLocks.getInstance().createLock("demo");
        storage.createDatabase("demo");
        DBLocks.getInstance().createLock("demo", "person");
        TableMetaData md = new TableMetaData();
        md.setClusterName("cluster1");
        md.setDatabaseName("demo");
        md.setTableName("person");
        ColumnMetaData age = new ColumnMetaData();
        age.setColumnName("age");
        age.setMaxSize(2);
        md.addColumn(age);
        storage.createTable("demo", "person", md);
    }

    @After
    public void tearDown() throws Exception {
        Files.walk(root.toPath()).sorted(Comparator.reverseOrder()).map(java.nio.file.Path::toFile).forEach(File::delete);
    }

    private static Request batch(String column) {
        Request r = new Request();
        r.setClusterName("cluster1");
        r.setDatabaseName("demo");
        r.setTableName("person");
        DataContainer dc = new DataContainer();
        dc.addValues(column, List.of("10", "20", "30"));
        r.setDataContainer(dc);
        return r;
    }

    private static CountRequest count(String column) {
        CountRequest q = new CountRequest();
        q.setClusterName("cluster1");
        q.setDatabaseName("demo");
        q.setTableName("person");
        Criteria c = new Criteria();
        c.setColumnName(column);
        c.setType(ConditionType.GT);
        c.setRhs("15");
        q.addCriteria(c);
        return q;
    }

    @Test
    public void batchToAnUnknownColumnCreatesNoFile() {
        try {
            storage.write(batch("c1"));
            fail("expected rejection");
        } catch (IllegalArgumentException expected) {
            // ok
        }
        assertFalse(new File(root, "cluster1/demo/person/c1").exists());
    }

    @Test
    public void queryOnKnownColumnWorksAndUnknownColumnIsRejected() {
        storage.write(batch("age"));
        assertEquals("Count matching the condition is 2", storage.processCount(count("age")).getResult());
        try {
            storage.processCount(count("salary"));
            fail("expected rejection");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }
}
