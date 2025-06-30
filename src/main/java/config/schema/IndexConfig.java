package config.schema;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class IndexConfig {
  Seed indexSeed;

  public IndexConfig(int indexProb) {
    Map<Integer, Integer> indexBuildProb = new HashMap<>();
    indexBuildProb.put(1, indexProb);
    indexBuildProb.put(0, 100 - indexProb);
    indexSeed = SeedUtils.initSeed(indexBuildProb);
  }
}
