import entity.Product;
import javafx.util.Pair;
import library.Loaders;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * See @Test for examples
 */
public class Main {

    // you can have multiple datasources
    private static DataSource ds1;
    private static DataSource ds2;

    public static void main(String[] args) throws IOException, SQLException {

        // load properties into datasource
        ds1 = new DataSource(Loaders.properties("datasource1.properties"));
        ds2 = new DataSource(Loaders.properties("datasource2.properties"));

        // simulate load
        //for (int i = 0; i < 100; i++) {
        new runner().start();
        //}
    }

    static class runner extends Thread {

        @Override
        public void run() {
            // connect to database (autocloses)
            try (DataConnection conn = ds1.getConnection()) {

                // create an objectmapper
                DataMapper<Product> dataMapper = new DataMapper<>(Product.class);

                // fetch a row
                List<Map<String, Object>> rows = conn.nativeSelect("SELECT * FROM products");
                List<Product> products = dataMapper.map(rows);

                // display the rows
                System.out.println(rows);

                // display it as products
                for (Product prod : products) {
                    System.out.println(prod);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            // connect to second database
            try (DataConnection conn = ds2.getConnection()) {

                // do something with second datasource

            } catch (SQLException e) {
                e.printStackTrace();
            }

            // connect to both at the same time
            try (DataConnection conn1 = ds1.getConnection()) {

                // access to conn1

                try (DataConnection conn2 = ds2.getConnection()) {

                    // access to conn1 and conn2
                    conn1.insertRow("destTable", conn2.selectRow("srcTable", new Pair<>("id", 1)));

                } catch (SQLException e) {
                    e.printStackTrace();
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}