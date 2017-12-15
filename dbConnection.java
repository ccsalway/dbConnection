package library.database;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import javafx.util.Pair;
import library.loaders.JsonLoader;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

public class DataSource {

    private static DataSource instance;
    private JSONObject config;
    private LinkedList<Connection> pool;
    private int connTimeout = 5;

    //---------------------------------------------------

    private DataSource(String configFile) throws IOException, ParseException {
        config = JsonLoader.load(configFile, StandardCharsets.UTF_8); // hardcoded charset!
        pool = new LinkedList<>();
    }

    public static DataSource getInstance(String configFile) throws IOException, ParseException {
        if (instance == null) {
            instance = new DataSource(configFile);
        }
        return instance;
    }

    //---------------------------------------------------

    private Connection _getConnection() {
        JSONArray servers = (JSONArray) config.get("servers");
        for (int i = 0; i < servers.size(); i++) {
            try {
                JSONObject server = (JSONObject) servers.get(i);

                String driver = server.get("driver").toString();
                String dsn = server.get("dsn").toString();
                String options = server.get("options").toString();
                String username = server.get("username").toString();
                String password = server.get("password").toString();

                if (options != null) {
                    dsn += "?" + options;
                }

                Class.forName(driver);
                return DriverManager.getConnection(dsn, username, password);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private synchronized Connection getConnection() throws SQLException {
        Connection conn = pool.pollFirst();
        if (conn == null || !conn.isValid(connTimeout)) {
            conn = _getConnection();
            if (conn == null) {
                throw new SQLException("err.sql_communication_exception");
            }
        }
        return conn;
    }

    private synchronized void putConnection(Connection conn) {
        pool.add(conn);
    }

    public int poolCount() {
        return pool.size();
    }

    //---------------------------------------------------

    public long insertRow(String tableName, Map<String, Object> vals) throws SQLException {
        List<String> colNames = new ArrayList<>();
        List<Object> colVals = new ArrayList<>();

        for (Map.Entry<String, Object> val : vals.entrySet()) {
            colNames.add(val.getKey());
            colVals.add(val.getValue());
        }

        String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableName,
                String.join(",", colNames),
                String.join(",", Collections.nCopies(colNames.size(), "?")));

        return executeInsert(sql, colVals);
    }

    public int updateRow(String tableName, Pair<String, Object> id, Map<String, Object> vals) throws SQLException {
        List<String> colNames = new ArrayList<>();
        List<Object> colVals = new ArrayList<>();

        for (Map.Entry<String, Object> e : vals.entrySet()) {
            colNames.add(e.getKey() + " = ?");
            colVals.add(e.getValue());
        }

        colVals.add(id.getValue());

        String sql = String.format("UPDATE %s SET %s WHERE %s = ? LIMIT 1",  // Nb. only updates 1 row (faster)
                tableName,
                String.join(",", colNames),
                id.getKey()
        );

        return executeUpdate(sql, colVals);
    }

    public int deleteRow(String tableName, Pair<String, Object> id) throws SQLException {
        String sql = String.format("DELETE FROM %s WHERE %s = ? LIMIT 1",  // Nb. only updates 1 row (faster)
                tableName,
                id.getKey()
        );

        List<Object> colVals = new ArrayList<>();
        colVals.add(id.getValue());

        return executeUpdate(sql, colVals);
    }

    public Map<String, Object> selectRow(String tableName, Pair<String, Object> id) throws SQLException {
        String sql = String.format("SELECT * FROM %s WHERE %s = ? LIMIT 1",
                tableName,
                id.getKey()
        );

        List<Map<String, Object>> rows = executeSelect(sql, Collections.singletonList(id.getValue()));

        if (!rows.isEmpty()) {
            return rows.get(0);
        }
        return null;
    }

    //---------------------------------------------------

    public long executeInsert(String sql, List<Object> vals) throws SQLException {
        Connection conn = getConnection();
        Boolean badConnection = false;
        try (PreparedStatement stmnt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            if (vals != null && !vals.isEmpty()) {
                for (int i = 0; i < vals.size(); i++) {
                    stmnt.setObject(i + 1, vals.get(i));
                }
            }
            if (!stmnt.execute()) {
                try (ResultSet generatedKeys = stmnt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        return generatedKeys.getLong(1);
                    }
                }
            }
        } catch (CommunicationsException | MySQLNonTransientConnectionException e) {
            badConnection = true;
            throw e;
        } finally {
            if (!badConnection) {
                putConnection(conn);
            }
        }
        return -1L;
    }

    public int executeUpdate(String sql, List<Object> vals) throws SQLException {
        Boolean badConnection = false;
        Connection conn = getConnection();
        try (PreparedStatement stmnt = conn.prepareStatement(sql)) {
            if (vals != null && !vals.isEmpty()) {
                for (int i = 0; i < vals.size(); i++) {
                    stmnt.setObject(i + 1, vals.get(i));
                }
            }
            if (!stmnt.execute()) {
                return stmnt.getUpdateCount();
            }
        } catch (CommunicationsException | MySQLNonTransientConnectionException e) {
            badConnection = true;
            throw e;
        } finally {
            if (!badConnection) {
                putConnection(conn);
            }
        }
        return -1;
    }

    public List<Map<String, Object>> executeSelect(String sql, List<Object> vals) throws SQLException {
        Connection conn = getConnection();
        Boolean badConnection = false;
        List<Map<String, Object>> rows = new LinkedList<>(); // keep rows ordered
        try (PreparedStatement stmnt = conn.prepareStatement(sql)) {
            if (vals != null && !vals.isEmpty()) {
                for (int i = 0; i < vals.size(); i++) {
                    stmnt.setObject(i + 1, vals.get(i));
                }
            }
            ResultSet rs = stmnt.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            Map<String, Object> row;
            while (rs.next()) {
                row = new LinkedHashMap<>(); // keep columns ordered
                for (int i = 0; i < meta.getColumnCount(); i++) {
                    row.put(meta.getColumnName(i + 1), rs.getObject(i + 1));
                }
                rows.add(row);
            }
        } catch (CommunicationsException | MySQLNonTransientConnectionException e) {
            badConnection = true;
            throw e;
        } finally {
            if (!badConnection) {
                putConnection(conn);
            }
        }
        return rows;
    }

    public Object executeScalar(String sql, List<Object> vals) throws SQLException {
        List<Map<String, Object>> rows = executeSelect(sql, vals);
        if (rows != null) {
            Map<String, Object> row = rows.get(0);
            if (row != null) {
                Map.Entry<String, Object> first = row.entrySet().iterator().next();
                return first.getValue();
            }
        }
        return null;
    }
}
