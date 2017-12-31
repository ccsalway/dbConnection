package library;

import com.mysql.jdbc.ReplicationDriver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;

class ConnectionPool {

    private Properties properties;
    private LinkedList<Connection> pool;

    public ConnectionPool() {
        this.pool = new LinkedList<>();
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public int poolSize() {
        return this.pool.size();
    }

    private Connection newConnection() throws SQLException {
        try {
            Class.forName(properties.getProperty("mysql.driver"));

            Properties props = new Properties();

            // We want this for failover on the slaves
            props.put("autoReconnect", "true");

            // We want to load balance between the slaves
            props.put("roundRobinLoadBalance", "true");

            // log in credentials
            props.put("user", properties.getProperty("mysql.username"));
            props.put("password", properties.getProperty("mysql.password"));

            return new ReplicationDriver()
                    .connect(properties.getProperty("mysql.dsn"), props);

        } catch (ClassNotFoundException e) {
            throw new SQLException(e.getMessage());
        }
    }

    public Connection getConnection(int timeout) throws SQLException {
        Connection conn = pool.pollFirst();
        if (conn == null || !conn.isValid(timeout)) {
            conn = newConnection();
        }
        return conn;
    }

    public void putConnection(Connection conn) throws SQLException {
        pool.add(conn);
    }

}