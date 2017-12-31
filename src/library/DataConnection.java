package library;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import javafx.util.Pair;

import java.sql.*;
import java.util.*;

public class DataConnection implements AutoCloseable {

    private ConnectionPool pool;
    private Connection conn;
    private int connTimeout;
    private boolean requeue = true;

    DataConnection(ConnectionPool pool, int connTimeout) throws SQLException {
        this.pool = pool;
        this.connTimeout = connTimeout;
        checkConnection();
    }

    DataConnection(ConnectionPool pool) throws SQLException {
        this(pool, 5);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    private boolean isConnectionValid() throws SQLException {
        return conn != null && !conn.isClosed() && conn.isValid(connTimeout);
    }

    private void checkConnection() throws SQLException {
        if (!isConnectionValid()) {
            conn = pool.getConnection(connTimeout);
        }
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }

    public int getPoolSize() {
        return pool.poolSize();
    }

    public void setReadOnly(boolean state) throws SQLException {
        conn.setReadOnly(state);
    }

    //---------------------------------------------------

    public Long insertRow(String tableName, Map<String, Object> vals) throws SQLException {
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

    public boolean updateRow(String tableName, Pair<String, Object> id, Map<String, Object> vals) throws SQLException {
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

        return executeUpdate(sql, colVals) == 1;
    }

    public boolean deleteRow(String tableName, Pair<String, Object> id) throws SQLException {
        String sql = String.format("DELETE FROM %s WHERE %s = ? LIMIT 1",  // Nb. only updates 1 row (faster)
                tableName,
                id.getKey()
        );

        List<Object> colVals = new ArrayList<>();
        colVals.add(id.getValue());

        return executeUpdate(sql, colVals) == 1;
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
        conn.setReadOnly(false);
        try (PreparedStatement stmnt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            if (vals != null && !vals.isEmpty()) {
                for (int i = 0; i < vals.size(); i++) {
                    stmnt.setObject(i + 1, vals.get(i));
                }
            }
            if (!stmnt.execute()) { // false if the first result is an update count or there is no result
                try (ResultSet generatedKeys = stmnt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        return generatedKeys.getLong(1);
                    }
                }
            }
        } catch (CommunicationsException | MySQLNonTransientConnectionException e) {
            requeue = false;
            throw e;
        }
        return -1L;
    }

    public int executeUpdate(String sql, List<Object> vals) throws SQLException {
        conn.setReadOnly(false);
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
            requeue = false;
            throw e;
        }
        return -1;
    }

    public int executeDelete(String sql, List<Object> vals) throws SQLException {
        return executeUpdate(sql, vals);
    }

    public List<Map<String, Object>> executeSelect(String sql, List<Object> vals) throws SQLException {
        conn.setReadOnly(true);
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
            requeue = false;
            throw e;
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

    //---------------------------------------------------

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    @Override
    public void close() throws SQLException {
        if (isConnectionValid()) {
            if (requeue) {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
                conn.setAutoCommit(true); // default setting
                pool.putConnection(conn);
            } else {
                conn.close();
            }
        }
    }

}