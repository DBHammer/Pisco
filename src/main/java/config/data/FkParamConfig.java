package config.data;

import config.schema.DistributionType;
import java.util.HashMap;
import lombok.Data;

@Data
public class FkParamConfig {
  private HashMap<DistributionType, Integer> distMap;
}
