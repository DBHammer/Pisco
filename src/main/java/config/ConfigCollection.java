package config;

import config.data.DataGenConfig;
import config.jdbc.DataSourceConfig;
import config.loader.LoaderConfig;
import config.replay.ReplayConfig;
import config.schema.SchemaConfig;
import config.schema.TransactionConfig;
import gen.data.format.DataFormat;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.Data;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
public class ConfigCollection {
  private DataSourceConfig datasource;
  private LoaderConfig loader;
  private SchemaConfig schema;
  private TransactionConfig transaction;
  private DataFormat dataFormat;
  private DataGenConfig dataGenerate;
  private Boolean cleanupOutputDirectory;
  private ReplayConfig replay;
  private FaultConfig fault;

  public static ConfigCollection parse(String configPath) throws IOException {
    //    System.out.println(configPath);
    if (configPath.endsWith("yml") || configPath.endsWith("yaml")) {
      return parseYAML(configPath);
    } else {
      throw new RuntimeException("不支持的文件格式");
    }
  }

  private static ConfigCollection parseYAML(String configPath) throws IOException {

    // 创建构造器
    Constructor constructor = new Constructor(ConfigCollection.class);

    // 添加类型描述
    constructor.addTypeDescription(new TypeDescription(LoaderConfig.class));

    // 解析
    Yaml yaml = new Yaml(constructor);
    InputStream is = Files.newInputStream(Paths.get(configPath));
    return yaml.load(is);
  }
}
