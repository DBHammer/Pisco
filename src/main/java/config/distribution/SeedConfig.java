package config.distribution;

import config.schema.DistributionType;
import lombok.Data;

@Data
public class SeedConfig {
  private int begin;
  private int end;
  private DistributionType distributionType;
  private String distributionParam;
}
