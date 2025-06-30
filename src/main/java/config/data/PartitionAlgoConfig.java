package config.data;

import lombok.Data;

@Data
public class PartitionAlgoConfig {
  private Double probOfStatic;
  private Double probOfDynamicExists;
}
