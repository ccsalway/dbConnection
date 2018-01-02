package library;

import annotations.Column;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;

public class DataMapper<T> {

    private Class clazz;
    private Map<String, Field> fields = new HashMap<>();
    Map<String, String> errors = new HashMap<>();

    public DataMapper(Class clazz) {
        this.clazz = clazz;

        List<Field> fieldList = Arrays.asList(clazz.getDeclaredFields());
        for (Field field : fieldList) {
            Column col = field.getAnnotation(Column.class);
            if (col != null) {
                field.setAccessible(true);
                fields.put(col.name(), field);
            }
        }
    }

    public T map(Map<String, Object> row) throws SQLException {
        try {
            T dto = (T) clazz.getConstructor().newInstance();
            for (Map.Entry<String, Object> entity : row.entrySet()) {
                if (entity.getValue() == null) {
                    continue;  // Don't set DBNULL
                }
                String column = entity.getKey();
                Field field = fields.get(column);
                if (field != null) {
                    field.set(dto, convertInstanceOfObject(entity.getValue()));
                }
            }
            return dto;
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
            throw new SQLException("Problem with data Mapping. See logs.");
        }
    }

    public List<T> map(List<Map<String, Object>> rows) throws SQLException {
        List<T> list = new LinkedList<>();

        for (Map<String, Object> row : rows) {
            list.add(map(row));
        }

        return list;
    }

    private T convertInstanceOfObject(Object o) {
        try {
            return (T) o;
        } catch (ClassCastException e) {
            return null;
        }
    }

}
