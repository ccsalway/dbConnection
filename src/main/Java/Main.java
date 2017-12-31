import javafx.util.Pair;
import library.DataConnection;
import library.DataSource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * See @Test for examples
 */
public class Main {

    // you can have multiple datasources
    private static final DataSource ds1 = new DataSource();
    private static final DataSource ds2 = new DataSource();

    public static void main(String[] args) throws IOException {

        // load properties into datasource
        Properties properties = new Properties();
        try (InputStream stream = Main.class.getResourceAsStream("application.properties")) {
            properties.load(stream);
            ds1.setProperties(properties);
        }

        // simulate load
        for (int i = 0; i < 10; i++) {
            new runner().start();
        }
    }

    static class runner extends Thread {

        @Override
        public void run() {
            // connect to database (autocloses)
            try (DataConnection conn = ds1.getConnection()) {

                // fetch a row
                Map<String, Object> row = conn.selectRow("tableName", new Pair<>("id", 1));

                // display the row
                System.out.println(row);

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

}