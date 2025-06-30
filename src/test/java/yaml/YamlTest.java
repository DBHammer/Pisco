package yaml;

import java.util.Map;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class YamlTest {
  @Test
  public void read() {
    Yaml yaml = new Yaml();
    Map<String, Map<String, Map<String, String>>> databases =
        yaml.load(getClass().getClassLoader().getResourceAsStream("database.yml"));
    for (String dbType : databases.keySet()) {
      System.out.println(dbType);
    }

    //        Map<String, Object> config = (Map<String, Object>) databases.get("mysql");
    //        for (String dbType : config.keySet()) {
    //            System.out.println(dbType);
    //        }
  }
}
