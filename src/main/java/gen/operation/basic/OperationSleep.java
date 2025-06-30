package gen.operation.basic;

import adapter.SQLFilter;
import gen.operation.enums.OperationType;

public class OperationSleep extends gen.operation.Operation {
  private final int sleepTime;

  public OperationSleep(int sleepTime) {
    super(
        new SQLFilter() {
          @Override
          public String filter(String sql) {
            return sql;
          }
        });
    setOperationType(OperationType.SLEEP);

    this.sleepTime = sleepTime;
  }

  @Override
  public String toSQLProtected() {
    return String.format("select sleep(%d);", this.sleepTime);
  }
}
