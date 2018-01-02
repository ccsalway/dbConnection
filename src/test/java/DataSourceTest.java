import javafx.util.Pair;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataSourceTest {

    private Properties properties;
    private DataSource ds;

    @BeforeAll
    void init() {
        properties = new Properties();
        try (InputStream stream = this.getClass().getClassLoader().getResourceAsStream("application.properties")) {
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @BeforeEach
    void setUp() throws SQLException {
        ds = new DataSource(properties);
    }

    @AfterEach
    void tearDown() throws SQLException {
        ConnectionPool pool = ds.getPool();
        for (Connection conn : pool.getConnections()) {
            if (pool.isConnectionValid(conn)) {
                conn.close();
            }
        }
    }

    @Test
    void testConnection() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            assertEquals(true, connection.isValid());
        }
    }

    @Test
    void testRequeue() throws SQLException {
        DataConnection connection = ds.getConnection();
        assertEquals(0, connection.getPoolSize());
        connection.close();
        assertEquals(1, connection.getPoolSize());
    }

    @Test
    void testClose() throws SQLException {
        DataConnection connection = ds.getConnection();
        assertEquals(0, connection.getPoolSize());
        connection.setRequeue(false);
        connection.close();
        assertEquals(0, connection.getPoolSize());
    }

    @Test
    void testConnectionAfterClose() throws SQLException {
        DataConnection connection = ds.getConnection();
        connection.setRequeue(false);
        connection.close();
        assertNull(connection.getConnection());
    }

    @Test
    void testAutoCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            assertEquals(true, connection.getConnection().getAutoCommit());
            connection.setAutoCommit(false);
            assertEquals(false, connection.getConnection().getAutoCommit());
        }
        assertEquals(true, ds.getPool().getLastConnection().getAutoCommit());
    }

    @Test
    void testReadOnly() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            assertEquals(false, connection.getConnection().isReadOnly());
            connection.getConnection().setReadOnly(true);
            assertEquals(true, connection.getConnection().isReadOnly());
        }
    }

    @Test
    void testInverseReadOnly() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            // set readOnly = true
            connection.getConnection().setReadOnly(true);

            // insert
            Map<String, Object> insertData = new HashMap<>();
            insertData.put("name", UUID.randomUUID().toString());

            Long id = connection.insertRow("products", insertData);
            assertNotNull(id);
        }
    }

    @Test
    void testTransactions() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            // insert
            Map<String, Object> insertData = new HashMap<>();
            insertData.put("name", UUID.randomUUID().toString());

            Long id = connection.insertRow("products", insertData);
            assertNotNull(id);

            // select insert
            Map<String, Object> insertRow = connection.selectRow("products", new Pair<>("id", id));
            assertEquals(insertData.get("name"), insertRow.get("name"));

            // update
            Map<String, Object> updateData = new HashMap<>();
            updateData.put("name", UUID.randomUUID().toString());

            boolean updated = connection.updateRow("products", new Pair<>("id", id), updateData);
            assertTrue(updated);

            // select update
            Map<String, Object> updateRow = connection.selectRow("products", new Pair<>("id", id));
            assertEquals(updateData.get("name"), updateRow.get("name"));

            // delete
            boolean deleted = connection.deleteRow("products", new Pair<>("id", id));
            assertTrue(deleted);
        }
    }

    @Test
    void testCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            Map<String, Object> vals = new HashMap<>();
            vals.put("name", UUID.randomUUID().toString());

            connection.setAutoCommit(false);

            Long id = connection.insertRow("products", vals);
            assertNotNull(id);
            connection.commit();

            connection.setAutoCommit(true);

            Map<String, Object> row = connection.selectRow("products", new Pair<>("id", id));
            assertEquals(vals.get("name"), row.get("name"));
        }
    }

    @Test
    void testDoubleCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            connection.setAutoCommit(false);

            // first transaction
            Map<String, Object> vals1 = new HashMap<>();
            vals1.put("name", UUID.randomUUID().toString());

            Long id1 = connection.insertRow("products", vals1);
            assertNotNull(id1);
            connection.commit();

            // second transaction
            Map<String, Object> vals2 = new HashMap<>();
            vals2.put("name", UUID.randomUUID().toString());

            Long id2 = connection.insertRow("products", vals2);
            assertNotNull(id2);
            connection.commit();

            connection.setAutoCommit(true);

            // check first transaction
            Map<String, Object> row1 = connection.selectRow("products", new Pair<>("id", id1));
            assertEquals(vals1.get("name"), row1.get("name"));

            // check second transaction
            Map<String, Object> row2 = connection.selectRow("products", new Pair<>("id", id2));
            assertEquals(vals2.get("name"), row2.get("name"));
        }
    }

    @Test
    void testBrokenDoubleCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            connection.setAutoCommit(false);

            // first transaction
            Map<String, Object> vals1 = new HashMap<>();
            vals1.put("name", UUID.randomUUID().toString());

            Long id1 = connection.insertRow("products", vals1);
            assertNotNull(id1);
            connection.commit();

            // check first transaction
            Map<String, Object> row1 = connection.selectRow("products", new Pair<>("id", id1));
            assertEquals(vals1.get("name"), row1.get("name"));

            // second transaction
            Map<String, Object> vals2 = new HashMap<>();
            vals2.put("name", UUID.randomUUID().toString());

            // the connection was set to read only by SELECT which can't be swapped back whilst in autocommit=false
            assertThrows(SQLException.class, () -> connection.insertRow("products", vals2));

            connection.setAutoCommit(true);
        }
    }

    @Test
    void testDoubleInsert() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            connection.setAutoCommit(false);

            // first transaction
            Map<String, Object> vals1 = new HashMap<>();
            vals1.put("name", UUID.randomUUID().toString());

            Long id1 = connection.insertRow("products", vals1);
            assertNotNull(id1);

            // check first transaction
            Map<String, Object> row1 = connection.selectRow("products", new Pair<>("id", id1));
            assertEquals(vals1.get("name"), row1.get("name"));

            // second transaction
            Map<String, Object> vals2 = new HashMap<>();
            vals2.put("name", UUID.randomUUID().toString());

            Long id2 = connection.insertRow("products", vals2);
            assertNotNull(id2);

            // check second transaction
            Map<String, Object> row2 = connection.selectRow("products", new Pair<>("id", id2));
            assertEquals(vals2.get("name"), row2.get("name"));

            // commit both inserts
            connection.commit();

            connection.setAutoCommit(true);
        }
    }

    @Test
    void testFixedDoubleCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            // first transaction
            connection.setAutoCommit(false);

            Map<String, Object> vals1 = new HashMap<>();
            vals1.put("name", UUID.randomUUID().toString());

            Long id1 = connection.insertRow("products", vals1);
            assertNotNull(id1);
            connection.commit();

            connection.setAutoCommit(true);

            // check first transaction
            Map<String, Object> row1 = connection.selectRow("products", new Pair<>("id", id1));
            assertEquals(vals1.get("name"), row1.get("name"));

            // second transaction
            connection.setAutoCommit(false);

            Map<String, Object> vals2 = new HashMap<>();
            vals2.put("name", UUID.randomUUID().toString());

            Long id2 = connection.insertRow("products", vals2);
            assertNotNull(id2);
            connection.commit();

            connection.setAutoCommit(true);

            // check second transaction
            Map<String, Object> row2 = connection.selectRow("products", new Pair<>("id", id2));
            assertEquals(vals2.get("name"), row2.get("name"));
        }
    }

    @Test
    void testRollback() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            Map<String, Object> vals = new HashMap<>();
            vals.put("name", UUID.randomUUID().toString());

            connection.setAutoCommit(false);

            Long id = connection.insertRow("products", vals);
            assertNotNull(id);
            connection.rollback();

            connection.setAutoCommit(true);

            Map<String, Object> row = connection.selectRow("products", new Pair<>("id", id));
            assertNull(row);
        }
    }

    @Test
    void testSelectBeforeCommit() throws SQLException {
        try (DataConnection connection = ds.getConnection()) {
            Map<String, Object> vals = new HashMap<>();
            vals.put("name", UUID.randomUUID().toString());

            connection.setAutoCommit(false);

            Long id = connection.insertRow("products", vals);
            assertNotNull(id);

            Map<String, Object> row = connection.selectRow("products", new Pair<>("id", id));
            assertEquals(vals.get("name"), row.get("name"));

            connection.commit();

            connection.setAutoCommit(true);
        }
    }
}