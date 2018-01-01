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
    private int initPoolSize = 5;

    public ConnectionPool(Properties properties) throws SQLException {
        this.pool = new LinkedList<>();
        this.properties = properties;
        assignProperties();
        initPool();
    }

    public void assignProperties() {
        if (properties.containsKey("datasource.connectTimeout")) {
            connTimeout = Integer.parseInt(properties.getProperty("datasource.connectTimeout"));
        }
        if (properties.containsKey("datasource.initPoolSize")) {
            initPoolSize = Integer.parseInt(properties.getProperty("datasource.initPoolSize"));
        }
    }

    public void initPool() throws SQLException {
        for (int i = 0; i < initPoolSize; i++) {
            putConnection(newConnection(), true);
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
            props.setProperty("connectTimeout", String.valueOf(connTimeout * 1000)); // milliseconds
            props.setProperty("user", properties.getProperty("datasource.username"));
            props.setProperty("password", properties.getProperty("datasource.password"));

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
        if (!isConnectionValid(conn)) {
            conn = newConnection();
        }
        return conn;
    }

    public void putConnection(Connection conn, boolean queue) throws SQLException {
        if (queue) {
            if (!conn.getAutoCommit()) {
                conn.rollback();
                conn.setAutoCommit(true); // default setting
            }
            pool.add(conn);
        } else {
            conn.close();
        }
    }

    public boolean isConnectionValid(Connection conn) throws SQLException {
        return conn != null && !conn.isClosed() && conn.isValid(connTimeout);
    }

    public Connection getLastConnection() {
        // for test purposes to ensure the connection was returned as expected
        return this.pool.pollLast();
    }

}