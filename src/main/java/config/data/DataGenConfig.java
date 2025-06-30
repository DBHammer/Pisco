package config.data;

import gen.data.param.PartitionStrategy;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
public class DataGenConfig {
  private PartitionAlgoConfig partitionAlgoConfig;
  private PkParamConfig pkParamConfig;
  private FkParamConfig fkParamConfig;
  private AttrParamConfig attrParamConfig;
  private DataRepoConfig dataRepoConfig;
  private PartitionStrategy partitionStrategy;
  private UkParamConfig ukParamConfig;

  public static DataGenConfig parse(String configPath) throws IOException {
    if (configPath.endsWith("xml")) {
      return parseXML(configPath);
    } else if (configPath.endsWith("yml") || configPath.endsWith("yaml")) {
      return parseYAML(configPath);
    } else {
      throw new RuntimeException("不支持的文件格式");
    }
  }

  private static DataGenConfig parseXML(String configPath) {
    throw new NotImplementedException();
  }

  private static DataGenConfig parseYAML(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(DataGenConfig.class));
    InputStream is = Files.newInputStream(Paths.get(configPath));
    return yaml.load(is);
  }
}
