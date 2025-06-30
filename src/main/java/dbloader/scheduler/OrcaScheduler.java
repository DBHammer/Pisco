package dbloader.scheduler;

import adapter.Adapter;
import config.ConfigCollection;
import dbloader.transaction.TransactionLoader;
import gen.operation.TransactionCaseRepo;
import gen.shadow.DBMirror;
import io.IOPath;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.jdbc.DataSourceUtils;

public class OrcaScheduler {
  private static final Logger logger = LogManager.getLogger(OrcaScheduler.class);

  private final int totalIter;
  private final ConfigCollection configColl;
  private final DBMirror dbMirror;
  private final IOPath ioPath;
  private final TransactionCaseRepo transactionCaseRepo;
  private final Adapter adapter;

  public OrcaScheduler(
      int iter,
      ConfigCollection configColl,
      DBMirror dbMirror,
      IOPath ioPath,
      TransactionCaseRepo transactionCaseRepo,
      Adapter adapter) {
    super();
    this.totalIter = iter;
    this.configColl = configColl;
    this.dbMirror = dbMirror;
    this.ioPath = ioPath;
    this.transactionCaseRepo = transactionCaseRepo;
    this.adapter = adapter;
  }

  @SneakyThrows
  public void schedule() {
    for (int iter = 0; iter < totalIter; iter++) {
      execute(iter);

      if (configColl.getLoader().isDump()) {
        adapter.dump(
            DataSourceUtils.getUser(),
            DataSourceUtils.getPassword(),
            DataSourceUtils.getDatabase(),
            String.format("%s/db-%d.sql", ioPath.dumpDir, iter));
      }
    }
  }

  private Map<Integer, Set<Integer>> execute(int iter) throws InterruptedException {
    // 创建事务加载器
    logger.info("Creating transaction loaders......");
    List<TransactionLoader> loaderList = new ArrayList<>();

    Map<Integer, Set<Integer>> globalAlteredRecordMap = new HashMap<>();

    // 用于主线程等待子线程结束
    CountDownLatch countDownLatch = new CountDownLatch(configColl.getLoader().getNumberOfLoader());

    // 创建事务加载器
    for (int loaderId = 0; loaderId < configColl.getLoader().getNumberOfLoader(); loaderId++) {
      TransactionLoader transactionLoader =
          new TransactionLoader(
              dbMirror,
              ioPath.traceDir,
              loaderId,
              adapter,
              transactionCaseRepo,
              configColl.getLoader(),
              countDownLatch,
              iter,
              globalAlteredRecordMap,
              true);

      loaderList.add(transactionLoader);
    }

    // 开始加载事务
    logger.info("Starting transaction loaders......");
    loaderList.forEach(Thread::start);

    int cnt = 0;
    long lastLoaderNumber = configColl.getLoader().getNumberOfLoader();
    while (!countDownLatch.await(20, TimeUnit.SECONDS)) {
      if (countDownLatch.getCount() < configColl.getLoader().getNumberOfLoader()
          && countDownLatch.getCount() == lastLoaderNumber) {
        cnt++;
        logger.info(String.format("Waiting for %d threads...", countDownLatch.getCount()));
        if (cnt > 100) {
          System.exit(-1);
        }
      } else {
        cnt = 0;
        lastLoaderNumber = countDownLatch.getCount();
      }
    }

    //        System.out.println(new Gson().toJson(globalAlteredRecordMap));

    return globalAlteredRecordMap;
  }
}
