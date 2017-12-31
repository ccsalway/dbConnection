package library;

import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException;
import javafx.util.Pair;

import java.sql.*;
import java.util.*;

public class DataConnection implements AutoCloseable {

    private ConnectionPool pool;
    private Connection conn;
    private boolean requeue = true;

    public DataConnection(ConnectionPool pool) throws SQLException {
        this.pool = pool;
        setConnection();
    }

    public Connection getConnection() {
        return conn;
    }

    private void setConnection() throws SQLException {
        conn = pool.getConnection();
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public void setRequeue(boolean requeue) {
        this.requeue = requeue;
    }

    public int getPoolSize() {
        return pool.poolSize();
    }

    public boolean isValid() throws SQLException {
        return pool.isConnectionValid(conn);
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

        return nativeInsert(sql, colVals);
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

        return nativeUpdate(sql, colVals) == 1;
    }

    public boolean deleteRow(String tableName, Pair<String, Object> id) throws SQLException {
        String sql = String.format("DELETE FROM %s WHERE %s = ? LIMIT 1",  // Nb. only updates 1 row (faster)
                tableName,
                id.getKey()
        );

        List<Object> colVals = new ArrayList<>();
        colVals.add(id.getValue());

        return nativeUpdate(sql, colVals) == 1;
    }

    public Map<String, Object> selectRow(String tableName, Pair<String, Object> id) throws SQLException {
        String sql = String.format("SELECT * FROM %s WHERE %s = ? LIMIT 1",
                tableName,
                id.getKey()
        );

        List<Map<String, Object>> rows = nativeSelect(sql, Collections.singletonList(id.getValue()));

        if (!rows.isEmpty()) {
            return rows.get(0);
        }
        return null;
    }

    //---------------------------------------------------

    public long nativeInsert(String sql, List<Object> vals) throws SQLException {
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

    public int nativeUpdate(String sql, List<Object> vals) throws SQLException {
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

    public int nativeDelete(String sql, List<Object> vals) throws SQLException {
        return nativeUpdate(sql, vals);
    }

    public List<Map<String, Object>> nativeSelect(String sql, List<Object> vals) throws SQLException {
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

    public List<Map<String, Object>> nativeSelect(String sql) throws SQLException {
        return nativeSelect(sql, new LinkedList<>());
    }

    public Object scalar(String sql, List<Object> vals) throws SQLException {
        List<Map<String, Object>> rows = nativeSelect(sql, vals);
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
        if (isValid()) {
            if (requeue) {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                    conn.setAutoCommit(true); // default setting
                }
                pool.putConnection(conn);
            } else {
                conn.close();
                conn = null;  // ensures no settings are passed on
            }
        }
    }
}