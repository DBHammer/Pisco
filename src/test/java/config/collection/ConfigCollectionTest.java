package config.collection;

import config.ConfigCollection;
import java.io.IOException;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class ConfigCollectionTest {

  @Test
  public void ConfigCollectionTest() throws IOException {
    ConfigCollection dataGenConfig = ConfigCollection.parse("config.yaml");
    Yaml yaml = new Yaml();
    System.out.println(yaml.dump(dataGenConfig));
  }
}
