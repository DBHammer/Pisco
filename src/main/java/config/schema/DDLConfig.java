package config.schema;

import gen.operation.enums.DDLType;
import gen.operation.enums.DistributeScheduleType;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class DDLConfig {
  private Map<String, Object> ddlType = null;
  private Map<String, Object> distributeScheduleType = null;
  private Seed ddlTypeSeed = null;
  private Seed distributeScheduleTypeSeed = null;

  public Seed getDDLTypeSeed() {
    if (ddlTypeSeed == null) {
      ddlTypeSeed = SeedUtils.initSeed(ddlType, DDLType.class);
    }
    return ddlTypeSeed;
  }

  public Seed getDistributeScheduleTypeSeed() {
    if (distributeScheduleTypeSeed == null) {
      distributeScheduleTypeSeed =
          SeedUtils.initSeed(distributeScheduleType, DistributeScheduleType.class);
    }
    return distributeScheduleTypeSeed;
  }
}
