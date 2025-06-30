package adapter;

import context.OrcaContext;
import gen.data.format.DataFormat;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.basic.OperationStart;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import trace.IsolationLevel;
import trace.ReadMode;
import trace.TraceLockMode;
import util.log.ExceptionLogger;

public class AdapterTiDB extends AdapterMySQL {
  // 配置路径，用于读取DBMS专属配置
  //    private final TiDBConfig tiDBConfig;

  /**
   * 如果需要用到涉及 dataFormat 的方法，那么必须设置，否则可以设为 null
   *
   * @param dataFormat dataFormat对象
   */
  public AdapterTiDB(DataFormat dataFormat) {
    super(dataFormat);

    if (OrcaContext.ioPath == null) {
      throw new RuntimeException("无法获取IOPath");
    }

    //        String configPath = String.format("%s/tidb.yml", OrcaContext.ioPath.configDir);
    //        try {
    //            this.tiDBConfig = TiDBConfig.parse(configPath);
    //        } catch (MalformedURLException | FileNotFoundException | DocumentException e) {
    //            throw new RuntimeException("TiDB配置获取失败");
    //        }
  }

  @Override
  public TraceLockMode calcTraceLockMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.NON_LOCK;
        case SELECT_LOCK_IN_SHARE_MODE:
          return TraceLockMode.SHARE_LOCK;
        case SELECT_FOR_UPDATE:
        case INSERT:
        case UPDATE:
        case DELETE:
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
    if (isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT:
          return ReadMode.CONSISTENT_READ;
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT_FOR_UPDATE:
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.LOCKING_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  @Override
  public void startTransaction(Connection conn, OperationStart operationStart) throws SQLException {
    String cmd;
    switch (operationStart.getStartType()) {
      case StartTransactionReadOnly:
        cmd = "START TRANSACTION READ ONLY;";
        break;
      case StartTransactionWithConsistentSnapshot:
        cmd = "START TRANSACTION WITH CONSISTENT SNAPSHOT;";
        break;
      case StartTransactionReadOnlyWithConsistentSnapshot:
        cmd = "START TRANSACTION READ ONLY WITH CONSISTENT SNAPSHOT;";
        break;
      case START:
      default:
        cmd = "START TRANSACTION;";
        break;
    }

    Statement stat = conn.createStatement();
    stat.execute(cmd);
    stat.close();
  }

  @Override
  public String writePredicate(int isolation, Operation operation, List<AttrValue> attrValues) {
    return null;
  }

  @Override
  public void handleException(SQLException e) {
    ExceptionLogger.error(e);
  }

  @Override
  public boolean isSnapshotPoint(
      IsolationLevel isolationLevel,
      OperationType opType,
      boolean isFirstReadOp,
      boolean isFirstWriteOp) {
    // 这行理论上不会被执行到，其判断在isCurrentRead里已经完成了
    //    Objects.requireNonNull(isolationLevel);
    return opType == OperationType.START;
  }

  @Override
  public boolean isCurrentRead(IsolationLevel isolationLevel, OperationType nowOpType) {

    if (nowOpType == OperationType.UPDATE
        || nowOpType == OperationType.DELETE
        || nowOpType == OperationType.INSERT) {
      return true;
    }
    return isolationLevel == IsolationLevel.READ_COMMITTED;
  }
}
