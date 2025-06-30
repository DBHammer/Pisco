package config.analysis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.Data;
import org.apache.commons.lang3.NotImplementedException;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

@Data
public class AnalysisConfig {
  private int intialRotation;
  private int terminalRotation;

  private int periodFootprintStatistic;
  private int privateBufferSize;
  private boolean enableHeap;
  private int versionChainPurgeLength;
  private int graphPurgeSize;
  private boolean trackWW;
  private boolean trackWR;
  private boolean trackRW;
  private boolean launchGC;
  private boolean takeVC;
  private boolean calculateInitialData;
  private boolean statisticFootprint;
  private boolean outputRuntime;
  private boolean verify;
  private boolean closeCycleDetection;
  private boolean pgOptimization;
  private boolean calculateGB;

  public static AnalysisConfig parse(String configPath) throws IOException {
    if (configPath.endsWith("xml")) {
      throw new NotImplementedException();
    } else if (configPath.endsWith("yml") || configPath.endsWith("yaml")) {
      return parseYAML(configPath);
    } else {
      throw new RuntimeException("不支持的文件格式");
    }
  }

  private static AnalysisConfig parseYAML(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(AnalysisConfig.class));
    InputStream is = Files.newInputStream(Paths.get(configPath));
    return yaml.load(is);
  }
}
