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
        md.setDatabaseName("demo");
        ok.setMetaData(md);
        ColumnResource.validate(ok);

        MetaRequest bad = new MetaRequest();
        bad.setType(MetaRequestType.DeleteDatabase);
        TableMetaData badMd = new TableMetaData();
        badMd.setDatabaseName("../../..");
        bad.setMetaData(badMd);
        assertRejected(() -> ColumnResource.validate(bad));

        MetaRequest badCol = new MetaRequest();
        badCol.setType(MetaRequestType.AddColumn);
        TableMetaData colMd = new TableMetaData();
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
}
