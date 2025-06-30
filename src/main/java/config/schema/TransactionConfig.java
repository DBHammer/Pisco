package config.schema;

import gen.operation.enums.OperationType;
import java.util.Map;
import lombok.Data;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class TransactionConfig {
  private Seed ifNoise = null;

  @Data
  public static class StartConfig {
    private Seed readOnlySeed;
    private Seed snapshotSeed;

    public StartConfig() {
      readOnlySeed = SeedUtils.initSeed(1, 2);
      snapshotSeed = SeedUtils.initSeed(1, 2);
    }
  }

  private Seed operationNumber = null;
  private Seed operationTypeSeed;
  private Map<String, Object> operationType = null;

  private StartConfig startConfig = new StartConfig();

  private Seed indexSeed = null;

  private DDLConfig DDL = new DDLConfig();

  private OperationConfig select = new OperationConfig(),
      update = new OperationConfig(),
      delete = new OperationConfig(),
      insert = new OperationConfig();

  public Seed getOperationTypeSeed() {
    if (operationTypeSeed == null) {
      operationTypeSeed = SeedUtils.initSeed(operationType, OperationType.class);
    }
    return operationTypeSeed;
  }

  public TransactionConfig() {
    if (ifNoise == null) {
      ifNoise = SeedUtils.initSeed(0, 1);
    }
  }
}
