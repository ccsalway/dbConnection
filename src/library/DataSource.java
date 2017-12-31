package library;

import java.sql.SQLException;
import java.util.Properties;

public class DataSource {

    private ConnectionPool pool = new ConnectionPool();

    public void setProperties(Properties properties) {
        this.pool.setProperties(properties);
    }

    public DataConnection getConnection(int timeout) throws SQLException {
        return new DataConnection(this.pool, timeout);
    }

    public DataConnection getConnection() throws SQLException {
        return new DataConnection(this.pool);
    }

}