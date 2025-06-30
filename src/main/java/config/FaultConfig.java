package config;

import gen.operation.enums.FaultType;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import lombok.Data;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class FaultConfig {
  private String toolDir;
  private int delayTime;
  private String dstIp;
  private String remotePort;
  private String localPort;
  private int timeout = 10;
  private String storagePath;
  private String processName = "sqlengine";
  private int killSignal = 9;
  private Map<String, Object> faultType = null;
  private Seed faultTypeSeed;
  private String networkDevice = "eth1";

  public Seed getFaultTypeSeed() {
    if (faultTypeSeed == null) {
      faultTypeSeed = SeedUtils.initSeed(faultType, FaultType.class);
    }
    return faultTypeSeed;
  }

  public static FaultConfig parse(String configPath) throws IOException {
    Yaml yaml = new Yaml(new Constructor(FaultConfig.class));
    File replay = new File(configPath);
    if (replay.exists()) {
      InputStream is = Files.newInputStream(Paths.get(configPath));
      FaultConfig faultConfig = yaml.load(is);
      is.close();
      return faultConfig;
    } else {
      return null;
    }
  }
}
