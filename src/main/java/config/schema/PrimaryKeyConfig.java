package config.schema;

import gen.data.type.DataType;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class PrimaryKeyConfig {
  Seed attributeNumber = null;
  Seed pkTypeSeed = null;
  Seed increaseSeed = null;

  Map<String, Object> dataType;

  public Seed getPkTypeSeed() {
    if (pkTypeSeed == null) {
      pkTypeSeed = SeedUtils.initSeed(dataType, DataType.class);
    }
    return pkTypeSeed;
  }

  public Seed getIncreaseSeed() {
    if (increaseSeed == null) {
      increaseSeed = SeedUtils.initSeed(0, 1);
    }
    return increaseSeed;
  }

  public PrimaryKeyConfig() {}
}
