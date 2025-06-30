package gen.operation.basic;

import adapter.SQLFilter;
import gen.operation.enums.OperationType;
import gen.operation.enums.StartType;
import lombok.Getter;

@Getter
public class OperationStart extends gen.operation.Operation {
  StartType startType;

  public OperationStart(StartType startType) {
    super(
        new SQLFilter() {
          @Override
          public String filter(String sql) {
            return sql;
          }
        });
    setOperationType(OperationType.START);
    this.startType = startType;
  }

  /**
   * 事务的起始语句
   *
   * @return
   */
  @Override
  public String toSQLProtected() {
    switch (startType) {
      case StartTransactionReadOnly:
        return "start transaction read only;";
      case StartTransactionWithConsistentSnapshot:
        return "start transaction with consistent snapshot;";
      case StartTransactionReadOnlyWithConsistentSnapshot:
        return "start transaction read only,with consistent snapshot;";
      case START:
      default:
        return "start transaction;";
    }
  }
}
