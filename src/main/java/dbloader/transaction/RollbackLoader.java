package dbloader.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import trace.OperationTrace;
import trace.OperationTraceType;

public class RollbackLoader {
  public static void loadRollbackOperation(
      Connection conn, OperationTrace operationTrace, Throwable exception) throws SQLException {

    // 操作生成时间戳,如果没有开始时间就拿预备时间代替
    if (operationTrace.getBeforeLoadTimestamp() != null) {
      operationTrace.setStartTime(operationTrace.getBeforeLoadTimestamp());
    } else {
      // 操作生成时间戳
      operationTrace.setStartTime(System.nanoTime());
    }

    operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
    operationTrace.setPredicateLock(exception.getMessage());
    // 执行回滚
    conn.rollback();
  }
}
