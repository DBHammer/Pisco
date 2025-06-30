package ana.thread;

import ana.graph.Dependency;
import ana.graph.DependencyGraph;
import ana.main.Config;
import ana.main.OrcaVerify;
import ana.output.OutputCertificate;
import ana.output.OutputStructure;
import ana.verify.adapter.Adapter;
import ana.version.*;
import ana.window.AnalysisWindow;
import ana.window.WriteSet;
import ana.window.profile.Profile;
import ana.window.profile.ProfileMap;
import gen.schema.Schema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replay.controller.ReplayController;
import trace.*;

/**
 * 分析相关的任务全部封装在这里
 *
 * @author like_
 */
public class AnalysisThread implements Runnable {

  private static final Logger logger = LogManager.getLogger(AnalysisThread.class);

  @Getter private static DispatchThread dispatchThread = null;

  /** 需要一个数据库适配器，不同数据库有不同的特性，分析是需要用不同的方法 */
  private static Adapter adapter = null;

  /** schema，服务于satisfy predicate */
  @Getter private static final Schema schema = null;

  public static void initialize(Adapter adapter) {
    AnalysisWindow.initialize();

    HistoryVersion.initialize();

    DependencyGraph.initialize();

    AnalysisThread.adapter = adapter;
  }

  @Override
  public void run() {

    while (true) { // 逐trace分析

      // 1.准备分析环境,不断调度trace
      // 1.1从分析窗口种获取第一条trace,如果为空说明analysis window为空，一定要调度trace
      // 1.2对于Mutual Exclusive来说，是将开始时间在当前trace所在事务的结束操作结束时间之前所有的trace都调度到window中
      // 1.3对于read consistency checking来说，
      // 1）为了找到当前trace的读一致性时间区间，事务的读一致性时间区间要准备好（有可能一个事务不存在获取读一致性时间区间的操作，即该事务不会以一致性读的方式读取任何数据，那么可能出现死循环，因此需要避免这个问题）
      // 2）是将开始时间在当前trace读一致性时间区间结束时间之前的所有trace调度到window TODO
      // 这里采用了保守的策略，将当前trace的结束时间之前的所有trace都调度到window，因为，trace的读一致性时间区间一定等于或者在trace时间区间之前
      // 1.4对于first update wins checking来说，是将结束时间在当前trace开始时间之前所有的trace调度到window中
      // 对于上一点，由于share trace buffer是按开始时间有序的，因此直接调度share trace buffer中的trace无法准备上述环境
      // 为了解决这个问题，需要给每一个Thread维护一个timestampLine，表示当前thread的trace被加入到analysis window的最大时间点
      // 同一个thread中的trace一定是严格有序的，准备first updater wins checking的环境相当于:不断调度trace到analysis
      // window，直到每一个thread的timestampLine大于当前trace的开始时间
      // 1.4
      // 第二种解决思路，将结束时间在当前trace开始时间之前的所有trace调度到window<=将开始时间在当前trace开始时间之前的所有trace调度到window<=调度线程按照trace的开始时间严格有序调度trace，因此，现有的方法原生支持first-updater-wins的验证，不需要引入timestampLine这一数据结构
      long startTS = System.nanoTime();

      OperationTrace operationTrace;
      dispatchThread = new DispatchThread();
      while (
      // 1.1
      (operationTrace = AnalysisWindow.peekFromCursor()) == null
          ||
          // 1.2
          !(ProfileMap.isPerfect(operationTrace.getTransactionID())
              && ProfileMap.getProfile(operationTrace.getTransactionID()).getEndFinishTimestamp()
                  <= AnalysisWindow.peekLastTrace().getStartTimestamp())
          ||
          // 1.3
          (!ProfileMap.isPerfect(operationTrace.getTransactionID())
              && ProfileMap.getProfile(operationTrace.getTransactionID())
                      .getConsistentReadTimeInterval()
                  == null)
          || operationTrace.getFinishTimestamp()
              >= AnalysisWindow.peekLastTrace().getStartTimestamp()
          ||
          // 1.5
          (operationTrace.getFinishTimestamp() >= OrcaVerify.shareTraceBuffer.getCordonLine())
      // 1.4
      // operationTrace.getStartTimestamp() >= TimestampLine.getMinTimestampLine()) {
      /*operationTrace.getStartTimestamp() >=  AnalysisWindow.peekLastTrace().getStartTimestamp()*/ ) {

        // 说明dispatchThread将所有的trace都调度到了analysis window中，不需要再继续调度trace了
        if (dispatchThread.isOver()) {
          // 当所有的trace调度完毕，那么每一个Version Chain的最后一个成员都具备检查WW依赖的条件，向所有Version Chain发起一次WW依赖检查
          for (String key : HistoryVersion.getHistoryVersion().keySet()) {
            Dependency.trackWW(key, HistoryVersion.getVersionChain(key));
          }

          break;
        } else {
          // 逐行调度trace
          dispatchThread.run();
        }
      }
      long finishTS = System.nanoTime();
      OrcaVerify.runtimeStatistic.increaseWhile1(finishTS - startTS);

      // 说明所有的trace都被analysis window分析完毕，结束分析
      if (AnalysisWindow.peekFromCursor() == null) {
        return;
      }
      OutputStructure.outputStructure(operationTrace.getOperationID());

      if (Config.VERIFY) {
        // 2.并行启动MutualExclusive、read consistency checking、first-updater-wins
        startTS = System.nanoTime();
        CountDownLatch countDownLatch = new CountDownLatch(3);

        // 每个trace都丢给每个线程分析，实际上并不是每一个trace都要接受每一种分析，判断逻辑在每一个线程中
        // new Thread(new MutualExclusive(countDownLatch)).start();
        new MutualExclusive(countDownLatch).run();
        // new Thread(new ReadConsistency(countDownLatch)).start();
        new ReadConsistency(countDownLatch).run();
        // new Thread(new FirstUpdaterWins(countDownLatch)).start();
        new FirstUpdaterWins(countDownLatch).run();

        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseVerify(finishTS - startTS);

        // 3.等待线程检查结束，
        try {
          countDownLatch.await();
        } catch (InterruptedException e) {
          logger.warn(e);
        }
      }

      // 4.移除window中的第一个trace
      // 4.1统计workload特征
      startTS = System.nanoTime();
      OrcaVerify.numberStatistic.statistic(operationTrace);

      // 4.2移除一条trace
      AnalysisWindow.removeFromCursor();
      finishTS = System.nanoTime();
      OrcaVerify.runtimeStatistic.increaseClear(finishTS - startTS);
    }
  }

  /**
   * 服务于检查锁之间是否互斥
   *
   * @author like_
   */
  static class MutualExclusive implements Runnable {

    /** 与Analysis Thread通信 */
    private final CountDownLatch countDownLatch;

    /** 分析线程中的线程池 */
    private final ExecutorService executor = null;

    public MutualExclusive(CountDownLatch countDownLatch) {
      super();
      this.countDownLatch = countDownLatch;
    }

    /** Output 服务于certificate信息展示，分别记录 当前正在分析的操作 当前正在分析操作所在事务的结束操作 与当前分析操作存在锁冲突的操作 */
    private OperationTrace operationTraceCertificate;

    private OperationTrace terminalOperationTraceCertificate;
    private OperationTrace nextOperationTraceCertificate;

    @Override
    public void run() {
      long startTS = System.nanoTime();
      // 0.假设已经在AnalysisWindow中准备好了分析环境

      // 1.取出头部的trace
      OperationTrace operationTrace = AnalysisWindow.peekFromCursor();
      Profile profile = ProfileMap.getProfile(operationTrace.getTransactionID());
      if (profile == null) {
        throw new RuntimeException("profile must not be null");
      }

      operationTraceCertificate = operationTrace;
      terminalOperationTraceCertificate =
          new OperationTrace(
              operationTrace.getThreadID(),
              operationTrace.getTransactionID().split(",")[1],
              "+inf");
      terminalOperationTraceCertificate.setStartTime(profile.getEndStartTimestamp());
      terminalOperationTraceCertificate.setFinishTime(profile.getEndFinishTimestamp());
      terminalOperationTraceCertificate.setOperationTraceType(profile.getEndType());

      OperationTrace nextOperationTrace;
      // 2.遍历analysis window，包括当前operationTrace，直到找到trace所在事务的结束操作，在遍历Analysis Window结束之前一定会推出循环
      ListIterator<OperationTrace> listIterator = AnalysisWindow.listIterator();
      while (!((operationTrace
              .getTransactionID()
              .equals((nextOperationTrace = listIterator.next()).getTransactionID()))
          && (profile.getEndType() == nextOperationTrace.getOperationTraceType()))) {}
      OperationTrace terminalOperationTrace = nextOperationTrace;

      // 3.遍历analysis window，包括当前operationTrace，直到找到开始时间在trace所在事务的结束操作结束时间之后的操作
      listIterator = AnalysisWindow.listIterator();
      while (listIterator.hasNext()
          && !(terminalOperationTrace.getFinishTimestamp()
              <= (nextOperationTrace = listIterator.next()).getStartTimestamp())) {
        OrcaVerify.numberStatistic.increaseMutualExclusive();

        // 3.1
        // 两个操作要在不同的事务中才满足进一步分析的条件
        // 两个操作是冲突操作才满足进一步分析的条件
        if (!operationTrace.getTransactionID().equals(nextOperationTrace.getTransactionID())
            && isMutualExclusive(operationTrace, nextOperationTrace, false)) {

          // 3.1.2 ME violation
          // 开始时间在trace结束时间之后的nextTrace才满足进一步分析的条件、结束时间在trace所在事务结束之前的nextTrace才满足进一步分析的条件
          if (operationTrace.getFinishTimestamp() < nextOperationTrace.getStartTimestamp()
              && nextOperationTrace.getFinishTimestamp() < profile.getEndStartTimestamp()) {
            nextOperationTraceCertificate = nextOperationTrace;
            isMutualExclusive(operationTrace, nextOperationTrace, true);
          }

          // 3.1.3推断依赖
          if (OperationTrace.isOverlapping(operationTrace, nextOperationTrace)
              && OperationTrace.isOverlapping(terminalOperationTrace, nextOperationTrace)) {
            OutputStructure.outputStructure(
                "ME:"
                    + operationTrace.getOperationID()
                    + "*"
                    + nextOperationTrace.getOperationID());
            ReplayController.sequenceDependency.addDependency(
                terminalOperationTrace.getOperationID(), nextOperationTrace.getOperationID());
          }
          if (OperationTrace.isOverlapping(operationTrace, nextOperationTrace)
              && !OperationTrace.isOverlapping(terminalOperationTrace, nextOperationTrace)) {
            OutputStructure.outputStructure(
                "ME:"
                    + nextOperationTrace.getOperationID()
                    + "*"
                    + operationTrace.getOperationID());
            ReplayController.sequenceDependency.addDependency(
                nextOperationTrace.getOperationID(), operationTrace.getOperationID());
          }
          if (!OperationTrace.isOverlapping(operationTrace, nextOperationTrace)
              && OperationTrace.isOverlapping(terminalOperationTrace, nextOperationTrace)) {
            OutputStructure.outputStructure(
                "ME:"
                    + terminalOperationTrace.getOperationID()
                    + "*"
                    + nextOperationTrace.getOperationID());
            ReplayController.sequenceDependency.addDependency(
                terminalOperationTrace.getOperationID(), nextOperationTrace.getOperationID());
          }
        }
      }

      // return之前通知Analysis Window
      countDownLatch.countDown();
      long finishTS = System.nanoTime();
      OrcaVerify.runtimeStatistic.increaseMutualExclusive(finishTS - startTS);
    }

    /**
     * 判断两个OperationTrace带有的所有锁是否互相排斥
     *
     * @param operationTrace current operation trace
     * @param nextOperationTrace next operation trace
     */
    private boolean isMutualExclusive(
        OperationTrace operationTrace, OperationTrace nextOperationTrace, boolean isCertificate) {

      // 1两个OperationTrace带有的lockMode只有互斥才满足进一步判断的条件
      if (!(operationTrace.getTraceLockMode() == TraceLockMode.SHARE_LOCK
                  && nextOperationTrace.getTraceLockMode() == TraceLockMode.EXCLUSIVE_LOCK
              || operationTrace.getTraceLockMode() == TraceLockMode.EXCLUSIVE_LOCK
                  && nextOperationTrace.getTraceLockMode() == TraceLockMode.SHARE_LOCK
              || operationTrace.getTraceLockMode() == TraceLockMode.EXCLUSIVE_LOCK
                  && nextOperationTrace.getTraceLockMode() == TraceLockMode.EXCLUSIVE_LOCK)
          || operationTrace.getOperationTraceType() == OperationTraceType.START
          || nextOperationTrace.getOperationTraceType() == OperationTraceType.START) {
        return false;
      }

      List<TupleTrace> readTupleList = operationTrace.getReadTupleList();
      List<TupleTrace> writeUnitList = operationTrace.getWriteTupleList();

      List<TupleTrace> nextTraceReadTupleList = nextOperationTrace.getReadTupleList();
      List<TupleTrace> nextTraceWriteTupleList = nextOperationTrace.getWriteTupleList();

      // executor = Executors.newCachedThreadPool();

      // 2
      // 2.1对两个OperationTrace加的tuple-level lock锁，判断任意两个锁之间是否兼容
      return isCompatible(readTupleList, nextTraceReadTupleList, isCertificate)
          || isCompatible(readTupleList, nextTraceWriteTupleList, isCertificate)
          || isCompatible(writeUnitList, nextTraceReadTupleList, isCertificate)
          || isCompatible(writeUnitList, nextTraceWriteTupleList, isCertificate);
      // 2.2predicate中带有的是共享锁，那么需要与排他锁进行判断是否互斥
      //            return isCompatible(predicateLock, nextTraceReadTupleList, isCertificate) ||
      //                    isCompatible(predicateLock, nextTraceWriteTupleList, isCertificate) ||
      //                    isCompatible(nextTracePredicateLock, readTupleList, isCertificate) ||
      //                    isCompatible(nextTracePredicateLock, writeUnitList, isCertificate);

      // 3.等待分析线程结束
      /*
      executor.shutdown();
      try {
      	while (!executor.awaitTermination(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
      	e.printStackTrace();
      }
       */
    }

    /**
     * 并行判断两个tupleList是否互斥
     *
     * @param tupleList1 first tuple list
     * @param tupleList2 second tuple list
     */
    private boolean isCompatible(
        List<TupleTrace> tupleList1, List<TupleTrace> tupleList2, boolean isCertificate) {
      if (tupleList1 == null || tupleList2 == null) {
        return false;
      }

      for (TupleTrace tupleTrace1 : tupleList1) {
        // executor.execute(new Thread(new Runnable() {
        // 在这里实现并行
        // 优点：线程间共享的数据比较集中，增加cache命中率
        // 缺点：线程过多，上下文切换开销大
        // public void run() {
        // TODO 可以做划分，每N个TupleTrace交给一个线程完成验证
        for (TupleTrace tupleTrace2 : tupleList2) {
          if (isCompatible(tupleTrace1, tupleTrace2, isCertificate)) {
            return true;
          }
        }
        // }
        // }));
      }
      return false;
    }

    /**
     * 检查两个TupleTrace带有的锁信息是否兼容，即检查tuple-level lock的兼容性
     *
     * @param tupleTrace1 first tuple trace
     * @param tupleTrace2 second tuple trace
     * @return true if the first tuple is compatible with the second tuple
     */
    private boolean isCompatible(
        TupleTrace tupleTrace1, TupleTrace tupleTrace2, boolean isCertificate) {
      try {
        if (tupleTrace1.getTable().equals(tupleTrace2.getTable())
            && tupleTrace1.getPrimaryKey().equals(tupleTrace2.getPrimaryKey())) {
          if (isCertificate) {
            OutputCertificate.outputRecordLockError(
                operationTraceCertificate,
                terminalOperationTraceCertificate,
                nextOperationTraceCertificate,
                tupleTrace1,
                tupleTrace2);
          }
          return true;
        }
        return false;
      } catch (NullPointerException e) {
        throw new RuntimeException("tupleTrace.table and tupleTrace.primaryKey never be null");
      }
    }
  }

  /**
   * 服务于读一致性验证
   *
   * @author like_
   */
  static class ReadConsistency implements Runnable {

    /** 与Analysis Thread通信 */
    private final CountDownLatch countDownLatch;

    public ReadConsistency(CountDownLatch countDownLatch) {
      super();
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      long startTS = System.nanoTime();
      // 0.假设已经在AnalysisWindow中准备好了分析环境

      // 1.取出头部的trace
      OperationTrace operationTrace = AnalysisWindow.peekFromCursor();
      List<TupleTrace> readTupleList = operationTrace.getReadTupleList();
      if (readTupleList == null) {
        // return之前通知Analysis Window
        countDownLatch.countDown();
        return;
      }

      // 2.准备分析的结构
      // 2.1获取当前trace所在事务的write set
      HashMap<String, Version> writeSet =
          WriteSet.getTransactionVersions(
              WriteSet.getReadConsistency(), operationTrace.getTransactionID());
      // 2.2获取读一致性时间区间Consistent Read Timestamp Interval[Start, Finish),建立一个伪version
      final Version consistentReadTimestampInterval = getConsistentReadTimeInterval();
      // 2.3如果读操作的read mode不是UNCOMMITTED_READ，那么都需要忽略未提交版本
      boolean isIgnoreUncommitted = operationTrace.getReadMode() != ReadMode.UNCOMMITTED_READ;
      // 2.4线程池
      // executor = Executors.newCachedThreadPool();

      // 3.对trace中读取的所有tuple做读一致性验证
      for (TupleTrace tupleTrace : readTupleList) {
        // executor.execute(new Thread(new Runnable() {
        // 在这里实现并行
        // 优点：线程间共享的数据比较集中，增加cache命中率
        // 缺点：线程过多，上下文切换开销大
        // public void run() {
        Version selfVersion;
        ArrayList<Version> candidateReadSet;

        // 3.1如果被读取的tuple已经被事务之前的写操作写了，那么检查读操作是否读取到自己产生的写
        if (writeSet != null && (selfVersion = writeSet.get(tupleTrace.getKey())) != null) {
          // 3.1.1如果事务无法读取到自己的写
          if (!selfVersion.isCompatible(tupleTrace.getValueMap())) {
            // 出错，事务无法读取到自己产生的写
            OutputCertificate.outputReadSelfError(operationTrace, tupleTrace, writeSet);
          }
        } else {
          // 3.2如果被读取的tuple没有被事务之前的写操作写，那么检查是否一致地读取到了别人产生的写

          // 3.2.1寻找candidate read set
          candidateReadSet =
              getCandidateReadSet(
                  tupleTrace.getKey(),
                  consistentReadTimestampInterval,
                  false,
                  isIgnoreUncommitted,
                  isIgnoreUncommitted);

          // 3.2.2看读结果是否与candidateReadSet中的某一个version兼容
          boolean isCompatible = false;
          Version compatibleVersion = null;
          for (Version candidateVersion : candidateReadSet) {
            if (candidateVersion.isCompatible(tupleTrace.getValueMap())) {
              if (!isCompatible) {
                isCompatible = true;
                compatibleVersion = candidateVersion;
              } else {
                // 存在两个version与之兼容
                compatibleVersion = null;
              }
            }
          }

          // 3.2.2检查candidateReadSet中是否只有一个Version与读操作兼容，如果是，则构建该Version的readList,服务于依赖追踪
          if (compatibleVersion != null) {
            compatibleVersion.addNewReader(
                operationTrace.getTransactionID(),
                tupleTrace.getKey(),
                operationTrace.getOperationID());
            //            OutputStructure.outputStructure(
            //                "CR:"
            //                    + compatibleVersion.getTransactionID()
            //                    + "*"
            //                    + operationTrace.getOperationID());
          }

          // debug 输出与读操作结果匹配的Candidate Version
          //          for (Version candidateVersion : candidateReadSet) {
          //            if (candidateVersion.isCompatible(tupleTrace.getValueMap())) {
          //              OutputStructure.outputStructure(
          //                  "CV:"
          //                      + operationTrace.getTransactionID()
          //                      + "*"
          //                      + candidateVersion.getTransactionID());
          //            }
          //          }

          // 3.2.3.1如果没有与读结果兼容的version，那么说明读结果出错
          if (!isCompatible) {
            // 出错
            OutputCertificate.outputReadOtherError(operationTrace, tupleTrace, candidateReadSet);
          }
        }
        // }
        // }));
      }

      // 4.等待分析线程结束
      /*
      executor.shutdown();
      try {
      	while (!executor.awaitTermination(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
      	e.printStackTrace();
      }
       */

      // 5.通知Analysis Window
      // return之前通知Analysis Window
      countDownLatch.countDown();
      long finishTS = System.nanoTime();
      OrcaVerify.runtimeStatistic.increaseReadConsistency(finishTS - startTS);
    }
  }

  /**
   * 服务于检查first update wins
   *
   * @author like_
   */
  static class FirstUpdaterWins implements Runnable {

    /** 与Analysis Thread通信 */
    private final CountDownLatch countDownLatch;

    public FirstUpdaterWins(CountDownLatch countDownLatch) {
      super();
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {

      // 0.假设已经在AnalysisWindow中准备好了分析环境

      // 1.取出analysis window头部的trace
      OperationTrace operationTrace = AnalysisWindow.peekFromCursor();
      List<TupleTrace> writeTupleList =
          operationTrace.getWriteTupleList(); // 只有成功的写才会放在writeTupleList

      // 2.1获取当前trace所在事务的profile
      Profile profile = ProfileMap.getProfile(operationTrace.getTransactionID());
      if (profile == null) {
        throw new RuntimeException("profile must not be null");
      }

      // 2.1FirstUpdaterWins验证检查
      // 2.1.1first-updater-wins只有在有写数据的条件才需要做检查
      // 2.1.2first-updater-wins只有在一致性读的模式下才需要做检查
      // 2.1.3判断数据库的隔离级别如果不做FirstUpdaterWins的检查，那么跳过检查
      if (writeTupleList == null
          || operationTrace.getReadMode() != ReadMode.CONSISTENT_READ
          || !adapter.doFirstUpdaterWins(profile.getIsolationLevel())) {
        // return之前通知Analysis Thread
        countDownLatch.countDown();
        return;
      }

      long startTS = System.nanoTime();

      // 2.2获取当前事务的读一致性时间区间
      Version consistentReadTimeInterval = profile.getConsistentReadTimeInterval();
      if (consistentReadTimeInterval == null) {
        throw new RuntimeException("consistentReadTimeInterval must not be null");
      }
      // 2.3事务读一致性快照的结束时间戳
      long consistentReadTimeFinishTimestamp = consistentReadTimeInterval.getFinishTimestamp();
      // 2.4操作的开始时间戳
      long startTimestamp = operationTrace.getStartTimestamp();

      // 3.针对trace写的每一个tuple去history
      // version中检查是否满足first-updater-wins的条件，即在事务读一致性快照的结束时间戳和操作的开始时间戳之间是否产生了新的version
      ListIterator<Version> listIterator;
      VersionChain versionChain;
      for (TupleTrace tupleTrace : writeTupleList) {
        OrcaVerify.numberStatistic.increaseFirstUpdaterWins();

        versionChain = HistoryVersion.getVersionChain(tupleTrace.getKey());
        listIterator = versionChain.getIterator();

        // 3.1找到一个已提交的version（不能是一个未提交版本，如果是我一个未提交版本不能确定性产生了first updater wins），
        // version的开始时间戳在consistentReadTimeFinishTimestamp之后，结束时间戳在startTimestamp之前
        Version nextVersion;
        while (VersionChain.hasNext(listIterator, true)) {

          nextVersion = listIterator.next();

          // 3.1.1如果nextVersion的结束时间小于事务开始操作的结束时间戳，那么该VersionChain之后的所有Version都不满足搜索条件，结束搜索
          if (nextVersion.getFinishTimestamp() < consistentReadTimeFinishTimestamp) {
            break;
          }

          // 3.1.2如果nextVersion的时间区间落在consistentReadTimeFinishTimestamp和startTimestamp之间，那么满足搜索条件
          if (nextVersion.getStartTimestamp() > consistentReadTimeFinishTimestamp
              && nextVersion.getFinishTimestamp() < startTimestamp) {
            // 违反了first-updater-wins
            OutputCertificate.outputSerializeAccessError(
                operationTrace, profile, nextVersion, versionChain);
          }

          // 3.1.3利用FUW推断依赖
          Version releaseSnapshotTimeInterval =
              new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
          if (Version.isOverlapping(nextVersion, consistentReadTimeInterval)
              && Version.isOverlapping(nextVersion, releaseSnapshotTimeInterval)) {
            OutputStructure.outputStructure(
                "FUW:" + nextVersion.getTransactionID() + "*" + operationTrace.getTransactionID());
          }
          if (Version.isOverlapping(nextVersion, consistentReadTimeInterval)
              && !Version.isOverlapping(nextVersion, releaseSnapshotTimeInterval)) {
            OutputStructure.outputStructure(
                "FUW:" + nextVersion.getTransactionID() + "*" + operationTrace.getTransactionID());
          }
          if (!Version.isOverlapping(nextVersion, consistentReadTimeInterval)
              && Version.isOverlapping(nextVersion, releaseSnapshotTimeInterval)) {
            OutputStructure.outputStructure(
                "FUW:" + operationTrace.getTransactionID() + "*" + nextVersion.getTransactionID());
          }
        }
      }

      // 4.return之前通知Analysis Thread
      countDownLatch.countDown();

      long finishTS = System.nanoTime();
      OrcaVerify.runtimeStatistic.increaseFirstUpdaterWins(finishTS - startTS);
    }
  }

  /**
   * 获取当前trace(即analysis window中的第一条trace)的一致性读时间区间
   *
   * @return the time interval
   */
  private static Version getConsistentReadTimeInterval() {
    // 1.分析线程准备好获取ConsistentReadTimeInterval的环境
    OperationTrace operationTrace = AnalysisWindow.peekFromCursor();
    Profile profile = ProfileMap.getProfile(operationTrace.getTransactionID());
    if (profile == null) {
      throw new RuntimeException("profile must not be null");
    }

    // 2.细节交给适配器完成
    return adapter.getConsistentReadTimeInterval(operationTrace, profile);
  }

  /**
   * 将version chain中的version按照与consistentReadTimestampInterval的时间关系和可见性将version划分为6类版本 Future
   * Version:开始时间戳在读一致性时间区间结束时间戳之后的版本 Overlapping Version:与读一致性时间区间重叠的版本 Pseudo Pivot
   * Version:结束时间戳恰好在读一致性时间区间开始时间戳之前的版本，需要一个搜索过程可能存在多个，对于可见的版本，将其升级为Pivot Version Pivot
   * Version:结束时间戳恰好在读一致性时间区间开始时间戳之前且可见的版本，可能不存在 Overlapping Pivot Version:与Pivot Version重叠的版本
   * Garbage Version:结束时间戳在Pivot Version开始时间戳之前的版本
   *
   * <p>1.返回key指定的version chain在consistentReadTimestampInterval上的candidate read set，服务于读一致性验证
   * 2.将垃圾版本剪枝，服务于version chain的清理工作
   *
   * <p>Phase 1:搜索Future Version、Overlapping Version Phase 2:搜索Pseudo Pivot Version、Pivot Version
   * Phase 3:搜索Overlapping Pivot Version Phase 4:搜索Garbage Version
   *
   * <p>在Version Chain中搜索version的每一个环节都增加了判断，是否忽略未提交版本，主要服务于version chain的清理工作的需求
   * 对于快照读的一致性和first-updater-wins的验证需求，搜索过程只考虑已提交版本 对于未提交读的一致性验证需求，搜索过程需要考虑未提交版本 对于清理Version
   * Chain的需求， 搜索Future Version、Overlapping Version（Phase1）必须要基于已提交版本，否则可能会错误清理已提交版本； 搜索pivot
   * Version的环节（Phase2）必须要基于已提交版本，否则可能会错误清理已提交版本，这样已提交读在搜索candidate read set时就会出现错误； 搜索与pivot
   * version重叠的version（Phase3）时要考虑未提交版本和已提交版本，否则会错误清理未提交版本，这样未提交读在搜索candidate read set时就会出现错误
   * 搜索在piovt version之前的version（Phase4）时要考虑所有版本，包括回滚版本，才能及时将不必要的版本进行清理
   *
   * @param key read key
   * @param consistentReadTimestampInterval read operation time interval
   * @param isPrune 是否需要清理version chain
   * @param isIgnoreUncommitted1 找与consistentReadTimestampInterval重叠的version（Phase1）和搜索Pivot
   *     Version（Phase2）时，是否忽略未提交版本
   * @param isIgnoreUncommitted2 找与pivot version重叠的version（Phase3）时，是否忽略未提交版本
   * @return read candidate set
   */
  public static ArrayList<Version> getCandidateReadSet(
      String key,
      Version consistentReadTimestampInterval,
      boolean isPrune,
      boolean isIgnoreUncommitted1,
      boolean isIgnoreUncommitted2) {
    ArrayList<Version> candidateReadSet = new ArrayList<>();
    VersionChain versionChain = HistoryVersion.getVersionChain(key);
    ListIterator<Version> listIterator = versionChain.getIterator();

    Version nextVersion = null;
    // 1.搜索Future Version、Overlapping Version
    // 循环退出有两种情况
    // 第一，version chain没有下一个version了
    // 第二，下一个version的结束时间恰好大于读一致性时间区间的开始时间
    long startTS = System.nanoTime();
    while (VersionChain.hasNext(listIterator, isIgnoreUncommitted1)
        && (nextVersion = listIterator.next()).getFinishTimestamp()
            > consistentReadTimestampInterval.getStartTimestamp()) {
      if (Version.isOverlapping(nextVersion, consistentReadTimestampInterval)) {
        candidateReadSet.add(nextVersion);
      }
    }
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseCRS1(finishTS - startTS);

    // 2.搜索Pseudo Pivot Version、Pivot Version
    // 2.1对于搜索Future Version、Overlapping
    // Version循环结束时的nextVersion（只要进入循环，那么nextVersion一定不为空），需要再进一步确认是否是第一个可能的Pseudo Pivot Version
    startTS = System.nanoTime();
    Version pseudoPivotVersion = null;
    if (nextVersion != null
        && nextVersion.getFinishTimestamp()
            <= consistentReadTimestampInterval.getStartTimestamp()) {
      pseudoPivotVersion = nextVersion;
    }

    // 2.2如果不存在pseudoPivotVersion，那么说明Version chain到末尾了，停止搜索
    if (pseudoPivotVersion == null) {
      if (isPrune) {
        // 2.2.1在做清理时，因为earliestReadTimeInterval可能太早了且没有初始版本的数据，导致candidatePivotVersion==null，直接返回即可
        return candidateReadSet;
      } else {
        // 2.2.2在做读一致性验证时，也可能candidatePivotVersion==null，比如，一个数据没有初始版本，version
        // chain中仅有一个版本，而读一致性时间区间恰好与读一致性时间区间重合
        return candidateReadSet;
      }
    }

    // 2.3需要迭代找到一个对于consistentReadTimestampInterval可见的pseudoPivotVersion作为真正的pivot
    // version，直到遍历version chain结束
    listIterator.previous(); // 回退一下，当前pseudoPivotVersion也要参与判断
    Visibility visibility;
    // 进入循环的两个条件
    // 第一，有下一个version作为pseudoPivotVersion
    // 第二，pseudoPivotVersion是未提交version
    while (VersionChain.hasNext(listIterator, isIgnoreUncommitted1)
        && (pseudoPivotVersion = listIterator.next()).getStatus() == VersionStatus.UNCOMMITTED) {

      // 2.3.1判断pseudoPivotVersion与consistentReadTimestampInterval的可见性
      visibility = Visibility.isVisible(pseudoPivotVersion, consistentReadTimestampInterval);

      if (visibility == Visibility.VISIBLE) {
        // 2.3.1.1如果确定可见，那么该pseudoPivotVersion就升级为pivotVersion
        break;
      } else if (visibility == Visibility.INVISIBLE) {
        // 2.3.1.2如果确定不可见，那么迭代到下一个version，不一定有下一个version
      } else if (visibility == Visibility.UNCERTAIN) {
        // 2.3.1.3如果不确定是否可见，将candidatePivotVersion加入candidate，迭代到下一个version,不一定有下一个version
        candidateReadSet.add(pseudoPivotVersion);
      }

      // 每一轮迭代结束，重置pivotVersion，用于退出循环后判断version chain是否遍历达到末尾
      pseudoPivotVersion = null;
    }
    // 2.3.2说明遍历version chain已经到达末尾，任然没有找到pivot version
    if (pseudoPivotVersion == null) {
      return candidateReadSet;
    }
    // 2.3.3将Pseudo Pivot Version升级为Pivot Version
    Version pivotVersion = pseudoPivotVersion;
    candidateReadSet.add(pivotVersion);
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseCRS2(finishTS - startTS);

    // 3.搜索Overlapping Pivot Version
    startTS = System.nanoTime();
    nextVersion = null; // 1,三处设置nextVersion为null，否则，会错误清理不该清理的版本
    while (VersionChain.hasNext(listIterator, isIgnoreUncommitted2)
        && (nextVersion = listIterator.next()).getFinishTimestamp()
            > pivotVersion.getStartTimestamp()) {
      // nextVersion一定与pivotVersion重叠
      candidateReadSet.add(nextVersion);
      nextVersion = null; // 2
    }
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseCRS3(finishTS - startTS);

    // 4.搜索Garbage Version
    startTS = System.nanoTime();
    if (isPrune) {
      // 循环退出，有两种可能，
      // 第一，version chain没有next(即nextVersion==null)；
      // 第二，当前nextVersion不满足条件(即version chain还有nextVersion，该nextVersion由1.3循环条件产生但不满足循环条件)
      while (nextVersion != null
          || (listIterator.hasNext() && (nextVersion = listIterator.next()) != null)) {

        // 4.1nextVersion符合清理条件，清理出version chain,并将nextVersion设为null(必须，从而上述循环才能继续生效)
        listIterator.remove();

        // 4.2如果该version是一个已提交版本（不包括初始版本）或者回滚版本，那么将writeSet(History version）中对应version移除
        if (nextVersion.getStatus() == VersionStatus.ROLLBACK
            || nextVersion.getStatus() == VersionStatus.COMMITTED
                && !(nextVersion.getTransactionID().equals("-1,-1"))) {
          // 4.2.1由于判断Pseudo Pivot Version的可见性需要用到事务结束操作的相关信息（profile），所以清理事务profile的时间点应该延后到History
          // Version中事务对应的所有版本都被清理完成的时间
          // 内部完成profile的清理工作，即当事务对应的WriteSet(HistoryVersion)中的Version全部都被清理，那么可以将对应的profile清理掉
          WriteSet.removeVersion(WriteSet.getHistoryVersion(), nextVersion.getTransactionID(), key);
        }

        nextVersion = null; // 3
      }
    }
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseCRS4(finishTS - startTS);

    return candidateReadSet;
  }
}
