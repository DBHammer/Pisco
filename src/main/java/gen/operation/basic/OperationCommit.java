package gen.operation.basic;

import adapter.SQLFilter;
import gen.operation.enums.OperationType;

public class OperationCommit extends gen.operation.Operation {
  public OperationCommit() {
    super(
        new SQLFilter() {
          @Override
          public String filter(String sql) {
            return sql;
          }
        });
    setOperationType(OperationType.COMMIT);
  }

  @Override
  public String toSQLProtected() {
    return "commit;";
  }
}
