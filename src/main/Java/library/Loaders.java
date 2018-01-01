package library;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Loaders {

    public static Properties properties(String fileName) throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = Loaders.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(stream);
        }
        return properties;
    }
}
