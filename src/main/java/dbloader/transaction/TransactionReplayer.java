package dbloader.transaction;

import static dbloader.transaction.TransactionLoader.isRecommit;

import adapter.Adapter;
import ana.output.ErrorStatistics;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import context.OrcaContext;
import exception.CommitException;
import exception.OperationFailException;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replay.controller.ReplayController;
import symbol.Symbol;
import trace.*;
import util.jdbc.DataSourceUtils;

@Getter
@Builder
public class TransactionReplayer extends Thread {
  public static final Logger logger = LogManager.getLogger(TransactionReplayer.class);
  private final int loaderId;
  private Adapter adapter;
  private final List<List<OperationTrace>> allTxns;
  private String traceDest;
  private int isolation;
  private final CountDownLatch countDownLatch;

  int iter;
  private long beforeLoadTimestamp;
  private long logBeforeLoadTimestamp;

  public void init() {
    adapter = DataSourceUtils.getAdapter();
    this.traceDest = String.format("%s/loader-%d", OrcaContext.ioPath.traceDir, loaderId);
  }

  public void setLog(Connection conn) throws SQLException {
    // log 隔离级别
    logger.info(
        String.format(
            "Logger-%d> ISOLATION LEVEL : %s",
            loaderId, IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));

    // 获取数据库元信息
    DatabaseMetaData databaseMetaData = conn.getMetaData();
    logger.info(
        String.format(
            "Logger-%d> Database Engine : %s %s",
            loaderId,
            databaseMetaData.getDatabaseProductName(),
            databaseMetaData.getDatabaseProductVersion()));
    logger.info(
        String.format(
            "Logger-%d> Driver : %s %s",
            loaderId, databaseMetaData.getDriverName(), databaseMetaData.getDriverVersion()));
    logger.info(String.format("Logger-%d>> initialized", loaderId));
  }

  @SneakyThrows
  @Override
  public void run() {

    Connection conn = DataSourceUtils.getJDBCConnection(true);
    @Cleanup
    TransactionTraceWriter traceWriter =
        new TransactionTraceWriter(traceDest, OrcaContext.configColl.getLoader().isTrace());

    // 关闭自动提交，即开启事务
    conn.setAutoCommit(false);
    // 初始化Session
    adapter.sessionConfig(conn);

    // 判断是否应当终止执行
    // 如果 execCountPerLoader 有效，则以此为依据
    // 按照指定的执行次数，随机抽取事务执行

    // execute

    List<OperationTrace> logs = new ArrayList<>();
    for (int i = 0; i < allTxns.size(); ++i) {
      try {
        // 处理TiDB #49811 导致的连接异常断开
        //        if (conn.isClosed()) {
        //
        //          conn = DataSourceUtils.getJDBCConnection(true);
        //          // 关闭自动提交，即开启事务
        //          conn.setAutoCommit(false);
        //          // 初始化Session
        //          adapter.sessionConfig(conn);
        //          logger.warn(
        //              "conn closed during to TiDB #49811, conn id is: "
        //                  + loaderId
        //                  + "; reopen: "
        //                  + !conn.isClosed());
        //        }
        logs.addAll(loadTransactionCase(allTxns.get(i), i, conn));
      } catch (SQLException e) {
        throw new RuntimeException("出错了!!!!!!!!!!", e);
      }
    }

    logger.info(String.format("Logger-%d>> transaction loaded", loaderId));
    logger.info(String.format("Logger-%d>> exit", loaderId));
    for (OperationTrace trace : logs) {
      traceWriter.writeOperationTrace(trace);
    }

    conn.close();

    // 通知主线程
    countDownLatch.countDown();
  }

  /**
   * 执行单个事务
   *
   * @param originTraces 要执行的事务的trace
   * @param conn 链接
   * @return 执行结果
   */
  private List<OperationTrace> loadTransactionCase(
      List<OperationTrace> originTraces, int transactionNo, Connection conn) throws SQLException {

    OperationTrace operationTrace = null;
    List<OperationTrace> logs = new ArrayList<>();

    //    if (originTraces.size() == 1) { // DDL之类的event先不回放
    //      return logs;
    //    }

    if (originTraces.size() > 1) {
      // 确定该loader的隔离级别
      this.isolation =
          IsolationLevel.enum2IntIsolation(
              originTraces
                  .get(0)
                  .getIsolationLevel()); // adapter.randIsolation(loaderConfig.getIsolation());
      conn.rollback();

      adapter.setTransactionIsolation(conn, this.isolation);
    }

    PreparedStatement pStat;
    try {
      // 逐一执行操作(由于已经关闭自动提交，直至commit时才会生效)
      // 对start检查执行顺序

      for (OperationTrace trace : originTraces) {

        if (!ReplayController.isExecute(trace.getOperationID())) {
          ReplayController.addFinishOp(trace.getOperationID());
          continue;
        }

        //                if (!OrcaContext.configColl.getLoader().isClearDebugInfo()){
        //                    System.out.println(trace.getOperationID());
        //                }

        ReplayController.isPause(trace); // 判断是否可以在当前时间段执行，不执行时锁住

        //                if (!OrcaContext.configColl.getLoader().isClearDebugInfo()){
        //                    System.out.println("after isPause:" + trace.getOperationID());
        //                }
        //        System.out.println(trace.getSql());

        operationTrace = new OperationTrace(trace);

        // control time

        operationTrace.setBeforeLoadTime(System.nanoTime());
        int writeNum = 1;

        String originSQL = trace.getSql();
        String rawSQL = adapter.getSQLFilter().filter(originSQL);
        operationTrace.setStartTime(System.nanoTime());

        OperationType type =
            TraceUtil.operationTraceType2OperationType(
                trace.getOperationTraceType()); // select / start / commit ..
        assert type != null;
        switch (type) {
          case SELECT:
            //            if (OrcaContext.configColl.getReplay().isSkipEmptySelect()) {
            //                            ReplayController.addFinishOp(trace.getOperationID());
            //                            continue;
            rawSQL = rawSQL.replace("for update", "");
            //            }

            if (!rawSQL.contains("pkId")) {
              rawSQL = rawSQL.replace("select ", "select pkId, ");
            }

            pStat = conn.prepareStatement(rawSQL);
            pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
            ResultSet resultSet = pStat.executeQuery();

            // 写读到的数据
            OperationLockMode readLockMode = OperationLockMode.SELECT;
            if (operationTrace.getTraceLockMode() == TraceLockMode.SHARE_LOCK) {
              readLockMode = OperationLockMode.SELECT_LOCK_IN_SHARE_MODE;
            } else if (operationTrace.getTraceLockMode() == TraceLockMode.EXCLUSIVE_LOCK) {
              readLockMode = OperationLockMode.SELECT_FOR_UPDATE;
            }
            operationTrace.setReadTupleList(parseResultSet(resultSet, conn, readLockMode));
            // 结束时间
            operationTrace.setFinishTime(System.nanoTime());

            //                        pStat.close();
            break;
          case UPDATE:
          case INSERT:
          case DELETE:
            if (operationTrace.getWriteTupleList() == null
                || operationTrace.getWriteTupleList().isEmpty()) {
              ReplayController.addFinishOp(trace.getOperationID());
              continue;
            }
            pStat = conn.prepareStatement(rawSQL);
            pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
            //                        operationTrace.setStartTime(System.nanoTime());
            writeNum = pStat.executeUpdate();
            //                        if (operationTrace.getSavepoint() != null) {
            //
            // adapter.setSavepoint(conn,operationTrace.getOperationID());
            //                        }
            // 结束时间
            operationTrace.setFinishTime(System.nanoTime());

            //                        pStat.close();
            break;
          case START:
          case DDL:
          case DistributeSchedule:
            //                        adapter.startTransaction(conn, new
            // OperationStart(StartType.START));
            if (rawSQL.isEmpty()) {
              continue;
            }
            pStat = conn.prepareStatement(rawSQL);
            pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
            pStat.execute();
            // 结束时间
            operationTrace.setFinishTime(System.nanoTime());
            //                        pStat.close();
            break;

          case COMMIT:
            //                        operationTrace.setStartTime(System.nanoTime());
            //            pStat = conn.prepareStatement("commit;");
            //
            // pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
            //            pStat.execute();
            conn.commit();
            // 结束时间
            operationTrace.setFinishTime(System.nanoTime());
            logger.info(
                String.format(
                    "Logger-%d, Transaction-%d> %s, %s",
                    loaderId,
                    transactionNo,
                    "COMMITTED",
                    IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));
            break;
          case ROLLBACK:
          default:
            //                        if (rawSQL != null && !rawSQL.contains("?")){
            //                            pStat = conn.prepareStatement(rawSQL);
            //                            if (rawSQL.toLowerCase().startsWith("select")){
            //                                pStat.executeQuery();
            //                            }
            //                            else{
            //                                pStat.executeUpdate();
            //                            }
            //
            //                        }
            //                        conn.rollback();

            pStat = conn.prepareStatement("rollback;");
            pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
            pStat.execute();

            operationTrace.setSql(null);
            operationTrace.setWriteTupleList(null);
            operationTrace.setReadTupleList(null);
            operationTrace.setTraceLockMode(null);
            //                        operationTrace.setStartTime(System.nanoTime());

            // 结束时间
            operationTrace.setFinishTime(System.nanoTime());
            logger.warn(
                String.format(
                    "Logger-%d, Transaction-%d> %s, %s",
                    loaderId,
                    transactionNo,
                    "ROLLBACK",
                    IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));
            break;
        }

        operationTrace = new OperationTrace(operationTrace);
        if (writeNum == 0) {
          operationTrace.setWriteTupleList(null);
        }
        //                if (OrcaContext.configColl.getLoader().isClearDebugInfo()) {
        //                    operationTrace.clearDebugInfo();
        //                }

        //                traceWriter.writeOperationTrace(operationTrace);
        ReplayController.addFinishOp(trace.getOperationID());
        logs.add(operationTrace);
        //                OperationSequence.increaseCounter(loaderId);
        if (TraceUtil.operationTraceType2OperationType(trace.getOperationTraceType())
            == OperationType.ROLLBACK) {
          break;
        }
      }

    } catch (Exception e) { // 操作无法执行或者更新失败时需要手动回滚

      if (OrcaContext.configColl.getReplay().getBuggyOpId() != null
          && operationTrace != null
          && OrcaContext.configColl
              .getReplay()
              .getBuggyOpId()
              .equals(operationTrace.getOperationID())) {
        ErrorStatistics.triggerBug = true;
      }

      // 如果是 SQLException，交给adapter做进一步处理
      e.printStackTrace();

      // 如果start就失败了，那就不记录这个事务
      if (operationTrace.getOperationTraceType() == OperationTraceType.START
          || operationTrace.getOperationTraceType() == OperationTraceType.DDL
          || operationTrace.getOperationTraceType() == OperationTraceType.DistributeSchedule
          || operationTrace.getOperationTraceType() == OperationTraceType.FAULT) {
        logger.warn(
            String.format("Logger-%d, Transaction-%d> %s", loaderId, transactionNo, "ROLLBACK"));
        return logs;
      }
      // 是否发生了自动回滚
      boolean needRecommit = isRecommit(e);
      if (e.getMessage() != null && e.getMessage().contains("Lock wait timeout exceeded")) {
        needRecommit = true;
      }

      if (e instanceof OperationFailException
          || e instanceof CommitException
          || e.getMessage().contains("Query execution was interrupted")
          || e.getMessage().contains("context canceled")
          || e.getMessage().contains("Correlate table error")
          || e.getMessage().contains("341222")
          || e.getMessage().contains("341244")
          || e.getMessage().contains("Update newest table")
          || e.getMessage().contains("Table definition has changed")
          || e.getMessage().contains("Duplicate entry")
          || e.getMessage().contains("COMMIT")) { // 处理TDSQL的特殊报错，统一进行回滚
        conn.rollback();
        needRecommit = false;
      }

      operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
      operationTrace.setException(e.getMessage());

      if (needRecommit) {
        try {
          // 如果数据库未自动回滚，且存在OrcaSavePoint，就提交savepoint前的部分
          operationTrace.setOperationTraceType(OperationTraceType.COMMIT);
          // 直接提交
          if (operationTrace.getStartTimestamp() == null) {
            operationTrace.setStartTime(System.nanoTime());
          }

          conn.commit();
          operationTrace.setException(e.getMessage());

          logger.warn(
              String.format(
                  "Logger-%d, Transaction-%d> %s, %s, %s",
                  loaderId,
                  transactionNo,
                  "COMMIT",
                  IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name(),
                  operationTrace.getException()));
          operationTrace.setFinishTime(System.nanoTime());
          logs.add(operationTrace);
          return logs;

        } catch (SQLException e1) {
          logger.warn("Rollback operation {} failed", operationTrace.getSql(), e1);
          operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
          conn.rollback();
          if (e1 instanceof SQLNonTransientConnectionException) {
            conn.close();
          }
          if (conn.isClosed()) {
            operationTrace.setFinishTime(System.nanoTime());
            operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
            logs.add(operationTrace);
            return logs;
          }
        }
      } else {
        conn.rollback();
        operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
      }

      if (e.getMessage() != null && e.getMessage().contains("XA_RBDEADLOCK")) {
        operationTrace.setFinishTime(System.nanoTime());

        conn.close();
        logs.add(operationTrace);
        return logs;
      }

      // 结束时间
      operationTrace.setFinishTime(System.nanoTime());
      logs.add(operationTrace);

      // 如果是 SQLException，交给adapter做进一步处理
      if (e instanceof SQLException) {
        if (!e.getMessage().contains("context canceled")
            && !e.getMessage().contains("Statement cancelled")
            && !e.getMessage().contains("Lock wait timeout exceeded")
            && !(e instanceof MySQLTimeoutException)) {
          adapter.handleException((SQLException) e);
        }
      } else if (e.getCause() instanceof SQLException) {
        SQLException e1 = (SQLException) e.getCause();
        if (!e1.getMessage().contains("context canceled")
            && !e1.getMessage().contains("Statement cancelled")
            && !e1.getMessage().contains("Lock wait timeout exceeded")) {
          adapter.handleException(e1);
        }
      }

      logger.warn(
          String.format(
              "Logger-%d, Transaction-%s> %s, %s",
              loaderId,
              operationTrace.getTransactionID(),
              "ROLLBACK",
              IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));
    }
    return logs;
  }

  private List<TupleTrace> parseResultSet(
      ResultSet resultSet, Connection conn, OperationLockMode traceLockMode) {

    // 用来储存推算出的pkId
    int pkId;
    List<TupleTrace> readTupleTraceList = new ArrayList<>();

    // 打印select结果
    ResultSetMetaData resultSetMetaData;
    try {
      resultSetMetaData = resultSet.getMetaData();

      // mariadb存在select为空的情况
      int columnsNumber = resultSetMetaData.getColumnCount();
      String tableName;
      if (columnsNumber >= 1) {
        tableName = resultSetMetaData.getTableName(1);
      } else {
        return new ArrayList<>();
      }

      while (resultSet.next()) {
        pkId = -1;

        // tupleTrace的一部分
        Map<String, String> valueMap = new HashMap<>();
        Map<String, String> realValueMap = new HashMap<>();

        for (int colInd = 1; colInd <= columnsNumber; colInd++) {

          String columnValue = resultSet.getString(colInd);
          // 按照类型读取
          switch (resultSetMetaData.getColumnType(colInd)) {
            case Types.INTEGER:
              columnValue = String.valueOf(resultSet.getInt(colInd));
              break;
            case Types.DECIMAL:
            case Types.DOUBLE:
              columnValue = String.valueOf(resultSet.getDouble(colInd));
              break;
            default:
          }

          String columnName = resultSetMetaData.getColumnName(colInd);

          if (columnName.contains(Symbol.PKID_ATTR_NAME)) {
            pkId = Integer.parseInt(columnValue);
          }
          //                    else{
          //                        pkId = -1;
          //                    }

          //          if (pkId == -1 && columnName.contains(Symbol.PK_ATTR_PREFIX)) {
          //            // 强行读取pkId,因为是个对同一行的点读，影响应该很小
          //            Statement statement = conn.createStatement();
          //            if (resultSetMetaData.getColumnType(colInd) == Types.VARCHAR) {
          //              columnValue = "\"" + columnValue + "\"";
          //            }
          //            String sql =
          //                "select `pkId` from "
          //                    + tableName
          //                    + " where `"
          //                    + columnName
          //                    + "` = "
          //                    + columnValue
          //                    + " "
          //                    + adapter.lockMode(traceLockMode);
          //            sql = adapter.getSQLFilter().filter(sql);
          //            ResultSet rs = statement.executeQuery(sql);
          //            if (rs.next()) {
          //              pkId = rs.getInt(1);
          //            }
          //          }

          // tupleTrace的一部分
          // trace
          valueMap.put(columnName, columnValue);
        }

        // tupleTrace
        // 移除主键属性
        //                    valueMap.keySet().removeIf(key ->
        // key.startsWith(Symbol.PK_ATTR_PREFIX));

        TupleTrace readTupleTrace =
            new TupleTrace(tableName, String.valueOf(pkId), valueMap, realValueMap);
        readTupleTraceList.add(readTupleTrace);
      }
    } catch (SQLException e) {
      //            e.printStackTrace();
      logger.error(e);
    }
    return readTupleTraceList;
  }
}
