import javafx.util.Pair;
import library.DataConnection;
import library.DataSource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

public class Main {

    private static DataSource ds = new DataSource();

    public static void main(String[] args) throws IOException {

        InputStream stream = Main.class.getClassLoader().getResourceAsStream("application.properties");

        Properties properties = new Properties();
        properties.load(stream);

        ds.setProperties(properties);

        try (DataConnection conn = ds.getConnection()) {

            HashMap<String, Object> insertData = new HashMap<>();
            Long id = conn.insertRow("tableName", insertData);

            Pair<String, Object> updateId = new Pair("id", id);
            HashMap<String, Object> updateData = new HashMap<>();
            boolean updated = conn.updateRow("tableName", updateId, updateData);

            Pair<String, Object> selectId = new Pair("id", id);
            Map<String, Object> row = conn.selectRow("tableName", selectId);

            Pair<String, Object> deleteId = new Pair("id", id);
            boolean deleted = conn.deleteRow("tableName", deleteId);

            //----------

            List<Object> insertVals = new LinkedList<>();
            Long id1 = conn.executeInsert("insert into table (name) values (?)", insertVals);

            List<Object> updateVals = new LinkedList<>();
            int updatedCount = conn.executeUpdate("update table set name = ? where id = ?", updateVals);

            List<Object> selectVals = new LinkedList<>();
            List<Map<String, Object>> rows = conn.executeSelect("select * from table where name like ?", selectVals);

            List<Object> deleteVals = new LinkedList<>();
            int deleteCount = conn.executeDelete("delete from table where id = ?", deleteVals);

            List<Object> scalarVals = new LinkedList<>();
            Object obj = conn.executeScalar("select * from table where id = ?", scalarVals);

            //---------

            conn.setAutoCommit(false);
            try {
                HashMap<String, Object> insertData2 = new HashMap<>();
                Long id2 = conn.insertRow("tableName", insertData2);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
            }

            //---------

            conn.setRequeue(false);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}