package config.schema;

import gen.data.type.DataType;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class AttributeConfig {
  Seed groupNumberSeed = new Seed();
  Seed attributeNumber = null;
  Seed attrType;

  Map<String, Object> dataType;

  public Seed getAttrType() {
    if (attrType == null) {
      attrType = SeedUtils.initSeed(dataType, DataType.class);
    }
    return attrType;
  }

  public AttributeConfig() {}
}
