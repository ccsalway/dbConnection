package library;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;

public class ConnectionPool {

    private LinkedList<Connection> pool;
    private Properties properties;
    private int connTimeout = 5;

    public ConnectionPool() {
        this.pool = new LinkedList<>();
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
        if (properties.containsKey("datasource.connectTimeout")) {
            connTimeout = Integer.parseInt(properties.getProperty("datasource.connectTimeout"));
        }
    }

    public LinkedList<Connection> getConnections() {
        return pool;
    }

    public int poolSize() {
        return this.pool.size();
    }

    private Connection newConnection() throws SQLException {
        try {
            Class.forName(properties.getProperty("datasource.driver"));

            Properties props = new Properties();

            if (properties.containsKey("datasource.connectTimeout")) {
                props.setProperty("connectTimeout", properties.getProperty("datasource.connectTimeout"));
            }

            props.put("user", properties.getProperty("datasource.username"));
            props.put("password", properties.getProperty("datasource.password"));

            //jdbc:mysql
            //jdbc:mysql:loadbalance
            //jdbc:mysql:replication
            return DriverManager.getConnection(properties.getProperty("datasource.dsn"), props);

        } catch (ClassNotFoundException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public Connection getConnection() throws SQLException {
        Connection conn = pool.pollFirst();
        if (conn == null || !conn.isValid(connTimeout)) {
            conn = newConnection();
        }
        return conn;
    }

    public void putConnection(Connection conn) {
        pool.add(conn);
    }

    public boolean isConnectionValid(Connection conn) throws SQLException {
        return conn != null && !conn.isClosed() && conn.isValid(connTimeout);
    }

    public Connection getLastConnection() {
        // for test purposes to ensure the connection was returned as expected
        return this.pool.pollLast();
    }

}