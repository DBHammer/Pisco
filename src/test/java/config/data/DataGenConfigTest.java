package config.data;

import java.io.IOException;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

public class DataGenConfigTest {

  @Test
  public void parseYAML() throws IOException {
    DataGenConfig dataGenConfig = DataGenConfig.parse("src/main/resources/model/config.yaml");
    Yaml yaml = new Yaml();
    System.out.println(yaml.dump(dataGenConfig));
  }
}
