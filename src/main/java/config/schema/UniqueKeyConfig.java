package config.schema;

import gen.data.type.DataType;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class UniqueKeyConfig {
  Seed attributeNumber = null;
  Seed ukTypSeed = null;

  Map<String, Object> dataType;

  public Seed getUkTypeSeed() {
    if (ukTypSeed == null) {
      ukTypSeed = SeedUtils.initSeed(dataType, DataType.class);
    }
    return ukTypSeed;
  }
}
