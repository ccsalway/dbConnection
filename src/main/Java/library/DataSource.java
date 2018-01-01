package library;

import java.sql.SQLException;
import java.util.Properties;

public class DataSource {

    private ConnectionPool pool;

    //------------------------------

    public DataSource(Properties properties) throws SQLException {
        this.pool = new ConnectionPool(properties);
    }

    //------------------------------

    public ConnectionPool getPool() {
        return pool;
    }

    public DataConnection getConnection() throws SQLException {
        return new DataConnection(this.pool);
    }

}