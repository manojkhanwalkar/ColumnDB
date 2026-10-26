package rest;

import org.junit.Before;
import org.junit.Test;
import query.*;

import javax.ws.rs.WebApplicationException;
import java.util.List;

import static org.junit.Assert.*;

public class InputValidationTest {

    @Before
    public void setUp() {
        ColumnResource.clusterName = "cluster1";
    }

    @Test
    public void acceptsPlainIdentifiers() {
        assertTrue(NameValidator.isValid("demo"));
        assertTrue(NameValidator.isValid("person_1"));
        assertTrue(NameValidator.isValid("user-id"));
    }

    @Test
    public void rejectsPathLikeNames() {
        for (String bad : new String[]{null, "", "..", "../x", "a/b", "a\\b", "person.meta", "/etc/passwd", "a\0b"}) {
            assertFalse(String.valueOf(bad), NameValidator.isValid(bad));
        }
    }

    private static Request batch(String cluster, String db, String table, String column) {
        Request r = new Request();
        r.setClusterName(cluster);
        r.setDatabaseName(db);
        r.setTableName(table);
        DataContainer dc = new DataContainer();
        dc.addValues(column, List.of("x"));
        r.setDataContainer(dc);
        return r;
    }

    private static void assertRejected(Runnable r) {
        try {
            r.run();
            fail("expected request to be rejected");
        } catch (WebApplicationException e) {
            assertEquals(400, e.getResponse().getStatus());
        }
    }

    @Test
    public void batchValidation() {
        ColumnResource.validate(batch("cluster1", "demo", "person", "age"));
        assertRejected(() -> ColumnResource.validate(batch("../../tmp", "demo", "person", "age")));
        assertRejected(() -> ColumnResource.validate(batch("cluster2", "demo", "person", "age")));
        assertRejected(() -> ColumnResource.validate(batch("cluster1", "demo", "person", "../../../../tmp/pwned")));
        assertRejected(() -> ColumnResource.validate(batch("cluster1", "demo", "person", "person.meta")));
    }

    @Test
    public void metaValidation() {
        MetaRequest ok = new MetaRequest();
        ok.setType(MetaRequestType.DeleteDatabase);
        TableMetaData md = new TableMetaData();
        md.setClusterName("cluster1");
        md.setDatabaseName("demo");
        ok.setMetaData(md);
        ColumnResource.validate(ok);

        MetaRequest bad = new MetaRequest();
        bad.setType(MetaRequestType.DeleteDatabase);
        TableMetaData badMd = new TableMetaData();
        badMd.setClusterName("cluster1");
        badMd.setDatabaseName("../../..");
        bad.setMetaData(badMd);
        assertRejected(() -> ColumnResource.validate(bad));

        MetaRequest badCol = new MetaRequest();
        badCol.setType(MetaRequestType.AddColumn);
        TableMetaData colMd = new TableMetaData();
        colMd.setClusterName("cluster1");
        colMd.setDatabaseName("demo");
        colMd.setTableName("person");
        ColumnMetaData cmd = new ColumnMetaData();
        cmd.setColumnName("../evil");
        cmd.setMaxSize(4);
        colMd.addColumn(cmd);
        badCol.setMetaData(colMd);
        assertRejected(() -> ColumnResource.validate(badCol));
    }

    @Test
    public void countRequestValidation() {
        CountRequest ok = new CountRequest();
        ok.setClusterName("cluster1");
        ok.setDatabaseName("demo");
        ok.setTableName("person");
        Criteria c = new Criteria();
        c.setColumnName("age");
        c.setType(ConditionType.GT);
        c.setRhs("10");
        ok.addCriteria(c);
        ColumnResource.validate(ok);

        CountRequest bad = new CountRequest();
        bad.setClusterName("../../etc");
        bad.setDatabaseName("demo");
        bad.setTableName("person");
        assertRejected(() -> ColumnResource.validate(bad));
    }

    private static MetaRequest column(MetaRequestType type, String cluster, String key, String name, int maxSize) {
        MetaRequest meta = new MetaRequest();
        meta.setType(type);
        TableMetaData md = new TableMetaData();
        md.setClusterName(cluster);
        md.setDatabaseName("demo");
        md.setTableName("person");
        ColumnMetaData cmd = new ColumnMetaData();
        cmd.setColumnName(name);
        cmd.setMaxSize(maxSize);
        md.getColumns().put(key, cmd);
        meta.setMetaData(md);
        return meta;
    }

    @Test
    public void columnDefinitionsMustBeUsable() {
        ColumnResource.validate(column(MetaRequestType.AddColumn, "cluster1", "age", "age", 4));
        // a 0 or negative width makes queries loop forever or crash
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.AddColumn, "cluster1", "age", "age", 0)));
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.CreateTable, "cluster1", "age", "age", -1)));
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.AddColumn, "cluster1", "age", "age", ColumnResource.MAX_COLUMN_SIZE + 1)));
        // DeleteColumn doesn't define a width
        ColumnResource.validate(column(MetaRequestType.DeleteColumn, "cluster1", "age", "age", 0));
    }

    @Test
    public void columnKeyMustMatchItsName() {
        // the key decides the metadata entry, the name decides the data file
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.DeleteColumn, "cluster1", "age", "salary", 4)));
    }

    @Test
    public void metaRequestsMustAddressThisCluster() {
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.AddColumn, "cluster2", "age", "age", 4)));
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.AddColumn, "../../x", "age", "age", 4)));
        assertRejected(() -> ColumnResource.validate(column(MetaRequestType.AddColumn, null, "age", "age", 4)));
    }
}
