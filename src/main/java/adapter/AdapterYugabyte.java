package adapter;

import gen.data.format.DataFormat;
import gen.operation.enums.OperationLockMode;
import java.sql.Connection;
import trace.ReadMode;
import trace.TraceLockMode;

public class AdapterYugabyte extends AdapterPostgreSQL {

  public AdapterYugabyte(DataFormat dataFormat) {
    super(dataFormat);
  }

  @Override
  public TraceLockMode calcTraceLockMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ
        || isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.NON_LOCK;
        case INSERT:
        case UPDATE:
        case DELETE:
        case SELECT_FOR_UPDATE:
          return TraceLockMode.EXCLUSIVE_LOCK;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  @Override
  public ReadMode calcTraceReadMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ
        || isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT_FOR_UPDATE:
          return ReadMode.LOCKING_READ;
        case INSERT:
        case SELECT:
        case UPDATE:
        case DELETE:
          return ReadMode.CONSISTENT_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }
}
