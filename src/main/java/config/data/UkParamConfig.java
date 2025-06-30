package config.data;

import config.schema.DistributionType;
import java.util.HashMap;
import lombok.Data;

/**
 * @program: Orca
 * @description:
 * @author: Ling Xiangrong
 * @create: 2023-10-25 18:21
 */
@Data
public class UkParamConfig {
  private HashMap<DistributionType, Integer> distMap;
}
