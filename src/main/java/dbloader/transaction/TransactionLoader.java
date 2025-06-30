package dbloader.transaction;

import adapter.Adapter;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException;
import config.loader.LoaderConfig;
import dbloader.transaction.command.Command;
import dbloader.transaction.savepoint.OrcaSavePoint;
import dbloader.util.TraceUtils;
import exception.CommitException;
import exception.NoProperRecordFoundException;
import exception.OperationFailException;
import gen.operation.Operation;
import gen.operation.TransactionCase;
import gen.operation.TransactionCaseRepo;
import gen.operation.basic.*;
import gen.operation.enums.OperationType;
import gen.shadow.DBMirror;
import gen.shadow.Record;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import trace.*;
import util.jdbc.DataSourceUtils;
import util.rand.RandUtil;
import util.rand.RandUtils;

@Getter
public class TransactionLoader extends Thread {
  public static final Logger logger = LogManager.getLogger(TransactionLoader.class);
  private static boolean isRunning = false;
  private final int loaderId;
  private final DBMirror dbMirror;
  private final Adapter adapter;
  private final TransactionCaseRepo transactionCaseRepo;
  private final LoaderConfig loaderConfig;
  private final String traceDest;
  private final boolean idAsSeed;
  private final int iter;
  private int isolation;
  private boolean gameOver = false; // 是否应该终止执行，当按时长运行时生效
  private final CountDownLatch mainCountDownLatch;
  private final Map<Integer, Set<Integer>> globalAlteredRecordMap;
  private final Map<Integer, Set<Integer>> alteredRecordMap = new HashMap<>();

  /**
   * 构造函数
   *
   * @param dbMirror miniShadow
   * @param traceDir trace输出目录
   * @param loaderId loaderId，类似于线程Id
   * @param adapter 适配器实例
   * @param transactionCaseRepo transactionCaseRepo
   * @param loaderConfig loader配置
   * @param mainCountDownLatch 来自主线程的线程计数器
   * @param idAsSeed 使用id作为种子可保证运行的事务的确定性
   */
  public TransactionLoader(
      DBMirror dbMirror,
      String traceDir,
      int loaderId,
      Adapter adapter,
      TransactionCaseRepo transactionCaseRepo,
      LoaderConfig loaderConfig,
      CountDownLatch mainCountDownLatch,
      int iter,
      Map<Integer, Set<Integer>> globalAlteredRecordSet,
      boolean idAsSeed) {
    this.dbMirror = dbMirror;
    this.loaderId = loaderId;
    this.adapter = adapter;
    this.transactionCaseRepo = transactionCaseRepo;
    this.loaderConfig = loaderConfig;
    this.traceDest = String.format("%s/loader-%d", traceDir, loaderId);
    this.mainCountDownLatch = mainCountDownLatch;
    this.globalAlteredRecordMap = globalAlteredRecordSet;
    this.idAsSeed = idAsSeed;
    this.iter = iter;
    this.isolation = 0;
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

    RandUtil randUtil;
    if (this.idAsSeed) {
      randUtil = new RandUtil(loaderId);
    } else {
      randUtil = new RandUtil();
    }

    Connection conn = DataSourceUtils.getJDBCConnection(true);
    @Cleanup
    TransactionTraceWriter traceWriter =
        new TransactionTraceWriter(traceDest, loaderConfig.isTrace());

    // 关闭自动提交，即开启事务
    conn.setAutoCommit(false);

    // 随机确定该loader的隔离级别
    this.isolation = adapter.randIsolation(loaderConfig.getIsolation());

    // OceanBase不支持 -> Not supported feature or function
    // conn.setTransactionIsolation 不适用于 OceanBase，因此放在adapter里面实现
    adapter.setTransactionIsolation(conn, this.isolation);

    setLog(conn);

    // 初始化Session
    adapter.sessionConfig(conn);

    // 判断是否应当终止执行
    // 如果 execCountPerLoader 有效，则以此为依据
    if (loaderConfig.getExecCountPerLoader() > 0) {
      // 按照预先给定的template序列顺序执行
      for (int transactionId = 0;
          transactionId < loaderConfig.getExecCountPerLoader() && !conn.isClosed();
          transactionId++) {
        int idx = transactionCaseRepo.getCaseSequence().get(loaderId).get(transactionId);
        TransactionCase transactionCase = transactionCaseRepo.get(idx);

        try {
          loadTransactionCase(transactionCase, transactionId, conn, traceWriter);
        } catch (IOException | SQLException e) {
          logger.warn(e.getMessage());
          break;
        } finally {
          isRunning = false;
        }
      }
    }
    // 否则 根据 execTimePerLoader 启动计时器以设置结束标志
    else {
      new Timer()
          .schedule(
              new TimerTask() {
                @Override
                public void run() {
                  gameOver = true;
                }
              },
              loaderConfig.getExecTimePerLoader() * 1000);
      int transactionId = 0;
      while (!gameOver && !conn.isClosed()) {
        TransactionCase transactionCase = randUtil.getRandElement(transactionCaseRepo);
        try {
          loadTransactionCase(transactionCase, transactionId++, conn, traceWriter);
        } catch (IOException | SQLException e) {
          throw new RuntimeException("出错了!!!!!!!!!!", e);
        } finally {
          isRunning = false;
        }
      }
    }

    logger.info(String.format("Logger-%d>> transaction loaded", loaderId));
    logger.info(String.format("Logger-%d>> exit", loaderId));

    synchronized (globalAlteredRecordMap) {
      for (Integer key : alteredRecordMap.keySet()) {
        globalAlteredRecordMap.putIfAbsent(key, new HashSet<>());
        globalAlteredRecordMap.get(key).addAll(alteredRecordMap.get(key));
      }
    }

    if (!conn.isClosed()) {
      conn.close();
    }

    traceWriter.close();
    // 通知主线程
    mainCountDownLatch.countDown();
  }

  private void loadTransactionCase(
      TransactionCase transactionCase,
      int transactionId,
      Connection conn,
      TransactionTraceWriter traceWriter)
      throws IOException, SQLException, InterruptedException {
    Operation operation = null;
    OperationTrace operationTrace = null;

    // 程序级别的SavePoint
    OrcaSavePoint orcaSavePoint = null;

    // 存储该事务的所有命令
    List<Command> commandList = new ArrayList<>();

    // 存储某条记录的存在与否信息
    // 如果delete一条数据，将它标记为不存在
    // 如果插入一条数据，将它标记为存在
    // key 为 tableName.pkId
    Map<String, Record> privateRecordMap = new HashMap<>();

    // 存储事务执行过程中涉及的需要加锁的Record
    Set<Record> needLockRecordSet = new LinkedHashSet<>();

    // 如果配置要求为每个事务随机选择隔离级别，则随机切换隔离级别
    if (loaderConfig.isRandIsolationPerTransaction()) {
      // 随机确定该loader的隔离级别
      this.isolation = adapter.randIsolation(loaderConfig.getIsolation());

      // OceanBase不支持 -> Not supported feature or function
      // conn.setTransactionIsolation 不适用于 OceanBase，因此放在adapter里面实现
      adapter.setTransactionIsolation(conn, this.isolation);
    }

    try {
      // 逐一执行操作(由于已经关闭自动提交，直至commit时才会生效)
      for (int ind = 0; ind < transactionCase.getOperationList().size(); ind++) {

        while (loaderConfig.isSerial() && isRunning) {
          Thread.sleep(100);
        }
        isRunning = true;

        operation = transactionCase.getOperationList().get(ind);

        // 实际执行的Operation，因为有时候会发生操作转换
        Operation execOperation = operation;
        // 线程ID，事务ID，操作ID
        operationTrace =
            new OperationTrace(
                String.format("%d-%d-%d", iter, 0, loaderId),
                String.valueOf(transactionId),
                String.valueOf(ind));

        // 加载前时间戳
        operationTrace.setBeforeLoadTime(System.nanoTime());

        switch (operation.getOperationType()) {
          case START:
            // 操作生成时间戳
            operationTrace.setStartTime(System.nanoTime());

            // 开始事务：设置读写模式和快照模式
            adapter.startTransaction(conn, (OperationStart) operation);

            // 将隔离级别存储到START操作的predicate中
            operationTrace.setIsolationLevel(IsolationLevel.intIsolation2enum(this.isolation));
            operationTrace.setSql(operation.toSQL());

            // 设置TraceLockMode和ReadMode
            TraceUtils.setLockModeAndReadModeForStartOperation(
                operationTrace, ((OperationStart) operation).getStartType());

            // 清空命令列表
            commandList.clear();
            break;
          case COMMIT:
            CommitLoader.loadCommitOperation(
                this, conn, operationTrace, needLockRecordSet, commandList);

            logger.info(
                String.format(
                    "Logger-%d, Transaction-%d> %s, %s",
                    loaderId,
                    transactionId,
                    "COMMITTED",
                    IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));
            break;
          case INSERT:
            List<Command> insertCommandList;
            try {
              insertCommandList =
                  InsertLoader.loadInsertOperation(
                      this,
                      execOperation,
                      conn,
                      operationTrace,
                      privateRecordMap,
                      needLockRecordSet,
                      alteredRecordMap);
            } catch (NoProperRecordFoundException e) {
              if (!loaderConfig.isAutoTransform()) {
                throw e;
              }
              execOperation = new OperationDelete((OperationInsert) execOperation);
              insertCommandList =
                  DeleteLoader.loadDeleteOperation(
                      this,
                      execOperation,
                      conn,
                      operationTrace,
                      privateRecordMap,
                      needLockRecordSet,
                      alteredRecordMap);
            }
            // 存入该事务的命令列表
            commandList.addAll(insertCommandList);
            break;
          case DELETE:
            List<Command> deleteCommandList;
            try {
              deleteCommandList =
                  DeleteLoader.loadDeleteOperation(
                      this,
                      execOperation,
                      conn,
                      operationTrace,
                      privateRecordMap,
                      needLockRecordSet,
                      alteredRecordMap);
            } catch (NoProperRecordFoundException e) {
              if (!loaderConfig.isAutoTransform()) {
                throw e;
              }
              execOperation = new OperationInsert((OperationDelete) execOperation);
              deleteCommandList =
                  InsertLoader.loadInsertOperation(
                      this,
                      execOperation,
                      conn,
                      operationTrace,
                      privateRecordMap,
                      needLockRecordSet,
                      alteredRecordMap);
            }
            // 存入该事务的命令列表
            commandList.addAll(deleteCommandList);
            break;
          case SELECT:
            SelectLoader.loadSelectOperation(
                this, execOperation,
                conn, operationTrace);
            break;
          case UPDATE:
            List<Command> updateCommandList =
                UpdateLoader.loadUpdateOperation(
                    this,
                    execOperation,
                    conn,
                    operationTrace,
                    privateRecordMap,
                    needLockRecordSet,
                    this.alteredRecordMap);
            // 存入该事务的命令列表
            commandList.addAll(updateCommandList);
            break;
          case DistributeSchedule:
            isRunning = false;
            List<Command> scheduleList =
                DistributeScheduleLoader.loadDistributeScheduleOperation(
                    this, execOperation, conn, operationTrace);
            commandList.addAll(scheduleList);
            break;
          case DDL:
            isRunning = false;
            List<Command> ddlList =
                DDLLoader.loadDDLOperation((OperationDDL) execOperation, conn, operationTrace);
            commandList.addAll(ddlList);
            break;
          case Fault:
            isRunning = false;
            List<Command> faultList =
                FaultInjector.injectFault((OperationFault) execOperation, conn, operationTrace);
            commandList.addAll(faultList);
            break;
          default:
            throw new RuntimeException("不支持的操作类型 " + operation.getOperationType().name());
        }

        // 结束时间
        operationTrace.setFinishTime(System.nanoTime());
        // 操作类型
        operationTrace.setOperationTraceType(
            TraceUtil.OperationType2TraceType(execOperation.getOperationType()));

        // trace隔离级别
        // 只有四种基本操作才执行该操作（因为没有适配其他操作，会抛出错误！！）
        if (execOperation.getOperationType() == OperationType.INSERT
            || execOperation.getOperationType() == OperationType.DELETE
            || execOperation.getOperationType() == OperationType.SELECT
            || execOperation.getOperationType() == OperationType.UPDATE
            || execOperation.getOperationType() == OperationType.DDL) {

          operationTrace.setTraceLockMode(
              adapter.calcTraceLockMode(this.isolation, execOperation.getOperationLockMode()));
          operationTrace.setReadMode(
              adapter.calcTraceReadMode(this.isolation, execOperation.getOperationLockMode()));

          if (RandUtils.randBoolByPercent(loaderConfig.getProbOfSavepoint())) {
            // 建立SavePoint
            if (orcaSavePoint != null) {
              conn.releaseSavepoint(orcaSavePoint.getSavepoint());
            }
            orcaSavePoint =
                new OrcaSavePoint(
                    adapter.setSavepoint(conn, operationTrace.getOperationID()),
                    privateRecordMap,
                    needLockRecordSet,
                    commandList);

            operationTrace.setSavepoint(orcaSavePoint.getSavepoint().getSavepointName());
          }
        }

        // 写入trace
        traceWriter.writeOperationTrace(operationTrace);

        isRunning = false;
        Thread.sleep(20);
      }
    } catch (SQLException
        | OperationFailException
        | CommitException
        | NoProperRecordFoundException e) { // 操作无法执行或者更新失败时需要手动回滚
      isRunning = false;
      // 如果start就失败了，那就不记录这个事务
      if (operation.getOperationType() == OperationType.START
          || operation.getOperationType() == OperationType.DDL
          || operation.getOperationType() == OperationType.Fault) {
        logger.warn(
            String.format(
                "Logger-%d, Transaction-%d> %s, %s",
                loaderId,
                transactionId,
                "ROLLBACK",
                IsolationLevel.intIsolation2enum(this.isolation).name()));
        return;
      }
      // 是否发生了自动回滚
      boolean needRecommit = isRecommit(e);
      if (e.getMessage() != null && e.getMessage().contains("Lock wait timeout exceeded")) {
        needRecommit = true;
      }

      if (e instanceof OperationFailException
          || e instanceof CommitException
          || e.getMessage().contains("Query execution was interrupted")
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
          if (orcaSavePoint != null) {
            CommitLoader.loadCommitOperation(this, conn, operationTrace, orcaSavePoint);
          } else { // 直接提交
            if (operationTrace.getStartTimestamp() == null) {
              operationTrace.setStartTime(System.nanoTime());
            }

            conn.commit();
            operationTrace.setException(e.getMessage());

            logger.warn(
                String.format(
                    "Logger-%d, Transaction-%d> %s, %s, %s",
                    loaderId,
                    transactionId,
                    "COMMIT",
                    IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name(),
                    operationTrace.getException()));
            operationTrace.setFinishTime(System.nanoTime());
            traceWriter.writeOperationTrace(operationTrace);
            return;
          }
        } catch (CommitException | SQLException e1) {
          logger.warn("Rollback operation {} failed", operationTrace.getSql(), e1);
          operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
          conn.rollback();
          if (e1 instanceof SQLNonTransientConnectionException) {
            conn.close();
          }
          if (conn.isClosed()) {
            operationTrace.setFinishTime(System.nanoTime());
            operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
            traceWriter.writeOperationTrace(operationTrace);
            traceWriter.close();
            return;
          }
        }
      } else {
        conn.rollback();
        operationTrace.setOperationTraceType(OperationTraceType.ROLLBACK);
      }

      if (e.getMessage() != null && e.getMessage().contains("XA_RBDEADLOCK")) {
        operationTrace.setFinishTime(System.nanoTime());

        traceWriter.writeOperationTrace(operationTrace);
        traceWriter.close();
        conn.close();
        System.out.println("connection close here");
        return;
      }

      // 结束时间
      operationTrace.setFinishTime(System.nanoTime());
      traceWriter.writeOperationTrace(operationTrace);

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
              "Logger-%d, Transaction-%d> %s, %s",
              loaderId,
              transactionId,
              "ROLLBACK",
              IsolationLevel.intIsolation2enum(conn.getTransactionIsolation()).name()));
    }
  }

  public static boolean isRecommit(Exception e) {
    boolean isRollback =
        e instanceof SQLTransactionRollbackException
            || e instanceof MySQLTimeoutException
            || e.getCause() instanceof SQLTransactionRollbackException
            || e.getCause() instanceof MySQLTransactionRollbackException
            || (e.getCause() != null
                && e.getCause().getCause() instanceof SQLTransactionRollbackException)
            || (e.getMessage() != null && e.getMessage().contains("XA_RBDEADLOCK"));
    // 是否需要执行回滚操作
    return !isRollback;
  }
}
