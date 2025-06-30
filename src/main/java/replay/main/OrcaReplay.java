package replay.main;

import static context.OrcaContext.configColl;
import static context.OrcaContext.ioPath;

import ana.buffer.PrivateTraceBuffer;
import ana.buffer.ShareTraceBufferHeap;
import ana.io.HandlerFactory;
import ana.main.Config;
import ana.main.OrcaVerify;
import ana.output.ErrorStatistics;
import ana.output.OutputCertificate;
import ana.thread.AnalysisThread;
import config.ConfigCollection;
import context.OrcaContext;
import dbloader.transaction.TransactionReplayer;
import dbloader.util.QueryExecutor;
import dbloader.util.SchemaLoader;
import io.IOPath;
import io.IOUtils;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import main.Main;
import main.Orca;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replay.controller.ReplayController;
import replay.controller.cascadeDetect.CascadeDelete;
import replay.controller.executecontrol.ExecuteFilterMutationAbs;
import replay.controller.executecontrol.ExecutionFilterCropTailTxn;
import replay.controller.util.ReduceMutationType;
import trace.OperationTrace;
import trace.TraceUtil;
import util.jdbc.DataSourceUtils;

public class OrcaReplay {
  private static final Logger logger = LogManager.getLogger(OrcaReplay.class);

  private static String dbName;
  private static String dbType;
  private static int loopNum;
  private static String configDir;
  private static String sourceDir;
  private static String outputDir;

  private static boolean restore = false;

  public static void main(String[] args) throws SQLException, InterruptedException, IOException {
    if (args.length == 4) {
      // 数据库的配置、原始数据、输出目录
      // args[0] 是 orca
      configDir = args[1];
      sourceDir = args[2];
      outputDir = args[3];

      replay();
    } else {
      Main.printUsageAndExit();
    }
  }

  private static void replay() throws IOException, InterruptedException, SQLException {

    // 初始化配置与输出路径,将IOPath存储到全局备用
    OrcaContext.ioPath = new IOPath(configDir, outputDir);

    // 解析配置
    configColl = ConfigCollection.parse(configDir);

    dbType = configColl.getDatasource().getPlatform();
    String url = configColl.getDatasource().getUrl();
    dbName = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));
    loopNum = 1;
    // 配置数据库信息（需要在DataSourceUtils使用之前配置）
    // 需要与c3p0配置文件的信息保持一致
    Orca.initDataSource(dbType, dbName);

    // 初始化配置和读取器
    Config.initialize(dbType, sourceDir, outputDir, "nontrxctl", false);
    // 把已有数据同步到新输出目录
    ioPath.copyDir(sourceDir, outputDir);
    HandlerFactory.initialize();
    ReplayController.init();

    // allTxn: 所有事务的集合
    List<List<List<OperationTrace>>> allTxn = new ArrayList<>();
    for (int i = 0; i < configColl.getLoader().getNumberOfLoader(); ++i) {
      PrivateTraceBuffer privateTraceBuffer = new PrivateTraceBuffer();
      if (privateTraceBuffer.loadAllTrace(i) > 0) {
        allTxn.add(trace2Txn(privateTraceBuffer));
      } else {
        configColl.getLoader().setNumberOfLoader(i);
      }
    }

    // 清理输出文件夹
    // 重新更新一下序列，运行分析，把依赖啥的都给加上去
    analyseCase();
    ErrorStatistics.pinError();
    ReplayController.initOperationSequence();
    ReplayController.initializeWithAllTxn(allTxn);
    //    ReplayController.initVersionGraph(allTxn);

    ReplayController.getOperationSequence().print();

    if (configColl.getReplay().isAnaOnly()) {
      return;
    }

    // 开始迭代
    // 1.ExecutionFilterCropTailTxn
    ExecuteFilterMutationAbs filter;

    //    filter = new ExecuteMeaninglessCrop();
    //    filter.initializeWithAllTxn(allTxn);

    //    ReplayController.setMeaninglessCleaner(filter);
    //    ReplayController.addExecuteFilter(filter);

    filter = new ExecutionFilterCropTailTxn();
    filter.initializeWithAllTxn(allTxn);
    filter.mutation();
    ReplayController.addExecuteFilter(filter);

    printMutationResult(allTxn);

    if (!configColl.getReplay().getReduceMutationType().isEmpty()) {
      for (ReduceMutationType type : configColl.getReplay().getReduceMutationType()) {
        try {
          mutation(type.getExecuteFilterClass().getDeclaredConstructor().newInstance(), allTxn);
        } catch (NoSuchMethodException
            | InvocationTargetException
            | InstantiationException
            | IllegalAccessException e) {
          logger.error(e);
        }
      }
    }

    // 执行所有的 allTxn
    checkUtilValid(allTxn);
    //        System.out.printf("Replay:%d/%d%n", bugTrigger, loopNum);
    System.exit(0);
  }

  private static void mutation(
      ExecuteFilterMutationAbs filter, List<List<List<OperationTrace>>> allTxn) {
    filter.initializeWithAllTxn(allTxn);
    ReplayController.addExecuteFilter(filter);
    boolean CascadeDeleteValid = OrcaContext.configColl.getReplay().isCascadeDeleteValid();
    try {
      while (!filter.isMutationEnd()) {
        filter.mutation();
        // 处理级联删除
        if (CascadeDeleteValid) {
          CascadeDelete.addIntoCurrentSet(filter.getDeleteOperationTraces());
        }

        int bugTrigger = checkValid(allTxn);
        // 如果没触发bug，则回滚当前的mutation，以及级联删除
        if (bugTrigger == 0) {
          filter.revertMutation();
          if (CascadeDeleteValid) {
            CascadeDelete.clearNewSet();
          }
        }
        // 持久化级联删除的部分
        if (CascadeDeleteValid) {
          CascadeDelete.addIntoHistory();
        }
        ReplayController.cleanMeaningless();
        printMutationResult(allTxn);
      }
    } catch (Exception e) {
      logger.error(e);
      //            e.printStackTrace();
    }
  }

  private static void printMutationResult(List<List<List<OperationTrace>>> allTxns) {
    Set<String> opSet = new HashSet<>();
    Set<String> txnSet = new HashSet<>();
    // 在输出结果前清理一下无意义的操作，也即没有实际操作的事务
    ReplayController.cropMeaninglessOp();

    for (List<List<OperationTrace>> thread : allTxns) {
      for (List<OperationTrace> txn : thread) {
        for (OperationTrace op : txn) {
          if (ReplayController.isExecute(op.getOperationID())) {
            opSet.add(op.getOperationID());
            txnSet.add(op.getTransactionID());
          }
        }
      }
    }
    String filterName = ReplayController.getLastFilterClassName();
    logger.info(
        String.format(
            "End Mutation %s , with %d transactions and %d operations.",
            filterName, txnSet.size(), opSet.size()));
  }

  private static int checkValid(List<List<List<OperationTrace>>> allTxn)
      throws IOException, SQLException, InterruptedException {
    // 读取 ObjectCollection
    logger.info("Reading ObjectCollection ...");

    List<String> schema = IOUtils.readString(sourceDir + "/schema/schema.sql");
    List<String> insertList = IOUtils.readString(sourceDir + "/init_data/dataInsert.sql");

    int bugTrigger = 0;
    // 重定位
    Config.initialize(dbType, outputDir, outputDir, "start", false);
    for (int i = 0; i < loopNum; ++i) {

      ReplayController.reset();
      ErrorStatistics.triggerBug = false;

      if (!configColl.getReplay().isAnaOnly()) {
        OrcaContext.ioPath.cleanForReload();
        runCase(schema, insertList, allTxn);
      }

      if (ErrorStatistics.triggerBug) {
        bugTrigger++;
      } else {
        analyseCase();
        //            if (ErrorStatistics.hasError()) {
        //                System.exit(0);
        //            }
        bugTrigger += ErrorStatistics.hasError() ? 1 : 0;
      }
    }
    return bugTrigger;
  }

  private static void checkUtilValid(List<List<List<OperationTrace>>> allTxn)
      throws IOException, SQLException, InterruptedException {
    // 读取 ObjectCollection
    logger.info("Reading ObjectCollection ...");

    List<String> schema = IOUtils.readString(sourceDir + "/schema/schema.sql");
    List<String> insertList = IOUtils.readString(sourceDir + "/init_data/dataInsert.sql");

    // 重定位
    Config.initialize(dbType, outputDir, outputDir, "first", false);
    for (int i = 0; i < configColl.getReplay().getLoopNumber(); i++) {
      ReplayController.reset();

      OrcaContext.ioPath.cleanForReload();
      runCase(schema, insertList, allTxn);

      if (ErrorStatistics.triggerBug) {
        break;
      } else {
        analyseCase();

        if (ErrorStatistics.hasError()) {
          logger.info("triggers the same bug");
        } else {
          logger.info("does not trigger the same bug");
        }

        if (ErrorStatistics.hasError() && configColl.getReplay().isLimitedEndValid()) {
          break;
        }
      }
    }

    ReplayController.printFinalSequence();
  }

  private static void runCase(
      List<String> schema, List<String> insertList, List<List<List<OperationTrace>>> allTxns)
      throws SQLException, InterruptedException {
    loadData2DB(dbType, dbName, schema, insertList);

    // 创建事务加载器
    logger.info("Creating transaction loaders......");
    List<TransactionReplayer> loaderList = new ArrayList<>();

    // 用于主线程等待子线程结束
    CountDownLatch countDownLatch = new CountDownLatch(configColl.getLoader().getNumberOfLoader());

    // 创建事务加载器
    for (int loaderId = 0; loaderId < configColl.getLoader().getNumberOfLoader(); loaderId++) {
      if (allTxns.get(loaderId).isEmpty()) {
        continue;
      }
      TransactionReplayer transactionReplayer =
          TransactionReplayer.builder()
              .loaderId(loaderId)
              .allTxns(allTxns.get(loaderId))
              .countDownLatch(countDownLatch)
              .build();

      transactionReplayer.init();
      loaderList.add(transactionReplayer);
    }

    // 开始加载事务
    logger.info("Starting transaction loaders......");
    loaderList.forEach(Thread::start);

    while (!countDownLatch.await(configColl.getLoader().getQueryTimeout() * 2L, TimeUnit.SECONDS)) {
      logger.info(String.format("Waiting for %d threads...", countDownLatch.getCount()));
      //            for (int i = 0; i < 5; i++) {
      //                Thread.sleep(500);
      //                ReplayController.moveCursor();
      //            }
      ReplayController.moveCursor();
    }

    logger.info("************Good Bye*************");
    QueryExecutor.executor.shutdownNow();
  }

  public static void loadData2DB(
      String dbType, String dbName, List<String> schema, List<String> insertList)
      throws SQLException {
    // 将schema导入数据库中
    logger.info(String.format("Loading schema to database: %s-%s", dbType, dbName));
    SchemaLoader.loadToDB(dbName, schema);

    Set<String> tableList = new HashSet<>();
    for (String sql : schema) {
      String tableName = sql.split(" ")[2];
      tableList.add(tableName);
    }

    if (restore) {
      logger.info(String.format("Restore data to database: %s-%s", dbType, dbName));
      SchemaLoader.restoreDB(dbName, tableList);
    } else {
      // 数据导入数据库
      logger.info(String.format("Loading initial data to database: %s-%s", dbType, dbName));
      DataSourceUtils.loadInertList(insertList);

      SchemaLoader.copyDB(dbName, tableList, schema);
      restore = true;
    }
  }

  private static void analyseCase() throws IOException {
    HandlerFactory.initialize();

    OrcaVerify.shareTraceBuffer = new ShareTraceBufferHeap();

    // output
    ErrorStatistics.initialize();
    OutputCertificate.initialize();
    // analysis
    AnalysisThread.initialize(Config.adapter);

    // 3.启动分析线程
    OrcaVerify.analysisThread = new AnalysisThread();
    OrcaVerify.analysisThread.run();
    //    new Thread(new PerformanceStatisticThread()).start();

    ErrorStatistics.outputStatistics();

    OrcaContext.ioPath.copyTraceDir();
  }

  /**
   * 同一个事务下的所有 OperationTrace --> operationTraceList
   *
   * @param privateTraceBuffer 一个线程里的所有内容
   * @return 各个事务以及它们包含的操作列表
   */
  private static List<List<OperationTrace>> trace2Txn(PrivateTraceBuffer privateTraceBuffer) {
    List<OperationTrace> operationTraceList = new ArrayList<>();
    List<List<OperationTrace>> txnTraceList = new ArrayList<>();

    OperationTrace operationTrace = privateTraceBuffer.removeTrace();
    // load trace
    int transactionId = 0;
    while (operationTrace != null) {

      while (operationTrace != null
          && TraceUtil.getTxnIdFromOpId(operationTrace.getTransactionID()) == transactionId) {
        operationTraceList.add(operationTrace);
        operationTrace = privateTraceBuffer.removeTrace();
      }
      if (!operationTraceList.isEmpty()) {
        txnTraceList.add(operationTraceList);
        operationTraceList = new ArrayList<>();
      }
      transactionId++;
    }
    return txnTraceList;
  }
}
