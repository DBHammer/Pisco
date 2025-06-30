package config.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import lombok.Data;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
public class ConsistentConfig {
  private Map<String, Map<String, List<Integer>>> succDependency = null;
  private Map<String, Map<String, List<Integer>>> failDependency = null;
  private int successTime = 0;

  public static ConsistentConfig parse(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(ConsistentConfig.class));
    File replay = new File(configPath);
    if (replay.exists()) {
      InputStream is = Files.newInputStream(Paths.get(configPath));
      ConsistentConfig consistentConfig = yaml.load(is);
      //            ConsistentConfig consistentConfig = new ObjectMapper().readValue(is,
      // ConsistentConfig.class);
      is.close();
      return consistentConfig;
    } else {
      return null;
    }
  }

  public static void dump(String configPath, ConsistentConfig consistentConfig) throws IOException {
    @Cleanup FileWriter writer = new FileWriter(configPath);
    OutputStream outputStream = Files.newOutputStream(Paths.get(configPath));
    ObjectMapper mapper = new ObjectMapper();
    mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, consistentConfig);
  }
}
