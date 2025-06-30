package trace;

import java.sql.Connection;

public enum IsolationLevel {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
  SERIALIZABLE;

  public static int enum2IntIsolation(IsolationLevel isolationLevel) {
    if (isolationLevel == null) {
      return 4;
    }
    switch (isolationLevel) {
      case READ_UNCOMMITTED:
        return Connection.TRANSACTION_READ_UNCOMMITTED;
      case READ_COMMITTED:
        return Connection.TRANSACTION_READ_COMMITTED;
      case REPEATABLE_READ:
        return Connection.TRANSACTION_REPEATABLE_READ;
      case SERIALIZABLE:
        return Connection.TRANSACTION_SERIALIZABLE;
      default:
        throw new RuntimeException("不支持的隔离级别");
    }
  }

  public static IsolationLevel intIsolation2enum(int isolationLevel) {
    switch (isolationLevel) {
      case Connection.TRANSACTION_READ_UNCOMMITTED:
        return READ_UNCOMMITTED;
      case Connection.TRANSACTION_READ_COMMITTED:
        return READ_COMMITTED;
      case Connection.TRANSACTION_REPEATABLE_READ:
        return REPEATABLE_READ;
      case Connection.TRANSACTION_SERIALIZABLE:
        return SERIALIZABLE;
      default:
        throw new RuntimeException("不支持的隔离级别:" + isolationLevel);
    }
  }
}
