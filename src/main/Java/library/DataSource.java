package library;

import java.sql.SQLException;
import java.util.Properties;

public class DataSource {

    private ConnectionPool pool = new ConnectionPool();

    public DataSource() {}

    public DataSource(Properties properties) {
        this.pool.setProperties(properties);
    }

    public void setProperties(Properties properties) {
        this.pool.setProperties(properties);
    }

    public ConnectionPool getPool() {
        return pool;
    }

    public DataConnection getConnection() throws SQLException {
        return new DataConnection(this.pool);
    }

}