package dbloader.util;

import gen.operation.enums.StartType;
import trace.OperationTrace;
import trace.ReadMode;
import trace.TraceLockMode;

public class TraceUtils {
  /**
   * 为START操作记录LockMode, ReadMode
   *
   * @param operationTrace operationTrace
   * @param startType START操作的startType属性
   */
  public static void setLockModeAndReadModeForStartOperation(
      OperationTrace operationTrace, StartType startType) {
    if (startType == StartType.START) {
      operationTrace.setTraceLockMode(TraceLockMode.EXCLUSIVE_LOCK);
      operationTrace.setReadMode(ReadMode.UNCOMMITTED_READ);
    } else if (startType == StartType.StartTransactionReadOnly) {
      operationTrace.setTraceLockMode(TraceLockMode.NON_LOCK);
      operationTrace.setReadMode(ReadMode.UNCOMMITTED_READ);
    } else if (startType == StartType.StartTransactionWithConsistentSnapshot) {
      operationTrace.setTraceLockMode(TraceLockMode.EXCLUSIVE_LOCK);
      operationTrace.setReadMode(ReadMode.CONSISTENT_READ);
    } else if (startType == StartType.StartTransactionReadOnlyWithConsistentSnapshot) {
      operationTrace.setTraceLockMode(TraceLockMode.NON_LOCK);
      operationTrace.setReadMode(ReadMode.CONSISTENT_READ);
    } else {
      throw new RuntimeException("不支持的操作类型");
    }
  }
}
