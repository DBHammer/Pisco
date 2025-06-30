package ana.window;

import ana.graph.DependencyGraph;
import ana.main.Config;
import ana.main.OrcaVerify;
import ana.version.HistoryVersion;
import ana.version.Version;
import ana.version.VersionStatus;
import ana.window.profile.Profile;
import ana.window.profile.ProfileMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import trace.OperationTrace;
import trace.ReadMode;
import trace.TraceLockMode;
import trace.TupleTrace;

/**
 * 分析所基于的窗口
 *
 * @author like_
 */
public class AnalysisWindow {
  /** 核心数据结构：分析窗口 */
  private static LinkedList<OperationTrace> window = null;

  /** 指示分析到window中的哪一条trace了 */
  private static int cursor;

  public static void initialize() {
    window = new LinkedList<>();

    cursor = 0;

    ProfileMap.initialize();

    TimestampLine.initialize();

    Active.initialize();
  }

  /**
   * 向analysis window尾部添加一条trace，并同步维护相关结构
   *
   * @param operationTrace trace
   */
  public static void addTrace(OperationTrace operationTrace) {
    // 1.添加一个新的trace到window
    long startTS = System.nanoTime();
    window.addLast(operationTrace);
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAWPrepare(finishTS - startTS);

    // 2.同步维护相关结构
    // 2.1transaction profile
    // 2.2History Version和Write Set(History Version)
    // 2.3Dependency Graph
    // 2.4maximum TimestampLine for each thread
    // 2.5active transaction
    Version unCommittedVersion;
    VersionStatus finalVersionStatus = null;
    Version finalVersion;
    Profile profile;
    switch (operationTrace.getOperationTraceType()) {
      case START:
        // 2.5.1添加一个活跃的事务
        startTS = System.nanoTime();
        Active.putTransaction(
            operationTrace.getTransactionID(),
            operationTrace.getStartTimestamp(),
            operationTrace.getFinishTimestamp());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseActive1(finishTS - startTS);

        // 2.1.1新构建trace所在事务的profile,并添加到ProfileMap中
        startTS = System.nanoTime();
        profile =
            new Profile(
                operationTrace.getTransactionID(),
                operationTrace.getStartTimestamp(),
                operationTrace.getFinishTimestamp(),
                operationTrace.getTraceLockMode() == TraceLockMode.NON_LOCK,
                operationTrace.getReadMode() == ReadMode.CONSISTENT_READ,
                operationTrace.getIsolationLevel());
        ProfileMap.addProfile(operationTrace.getTransactionID(), profile);
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseProfileMap1(finishTS - startTS);

        // 2.3.1添加到DependencyGraph
        startTS = System.nanoTime();
        DependencyGraph.addProfile(profile);
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseDG1(finishTS - startTS);
        break;

      case SELECT:
        break;

      case UPDATE:
      case INSERT:
      case DELETE:
        // 2.1.2设置trace所在事务为读写事务
        startTS = System.nanoTime();
        ProfileMap.setReadWrite(operationTrace.getTransactionID());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseProfileMap2(finishTS - startTS);

        // 2.2.1抽取operationTrace产生的所有unCommittedVersion同步加入到historyVersion和writeSet（History Version）
        startTS = System.nanoTime();
        List<TupleTrace> writeTupleList = operationTrace.getWriteTupleList();
        if (writeTupleList != null) {
          for (TupleTrace tupleTrace : writeTupleList) {
            // 2.2.1.1抽取operationTrace（写操作）中所有可能的unCommittedVersion
            long startTS1 = System.nanoTime();
            unCommittedVersion =
                new Version(
                    operationTrace.getTransactionID(),
                    operationTrace.getStartTimestamp(),
                    operationTrace.getFinishTimestamp(),
                    tupleTrace.getValueMap(),
                    operationTrace.getOperationID(),
                    operationTrace.getOperationTraceType(),
                    operationTrace.getTraceFile(),
                    operationTrace.getSql());
            long finishTS1 = System.nanoTime();
            OrcaVerify.runtimeStatistic.increaseHV11(finishTS1 - startTS1);

            // 2.2.1.2将unCommittedVersion维护到WriteSet中
            startTS1 = System.nanoTime();
            WriteSet.addVersion(
                WriteSet.getHistoryVersion(),
                operationTrace.getTransactionID(),
                tupleTrace.getKey(),
                unCommittedVersion);
            finishTS1 = System.nanoTime();
            OrcaVerify.runtimeStatistic.increaseHV12(finishTS1 - startTS1);

            // 2.2.1.3同步地将unCommittedVersion维护到History Version中，注意historyVersion和writeSet（History
            // Version）被加入的是同一个version对象
            startTS1 = System.nanoTime();
            HistoryVersion.addVersion(tupleTrace.getKey(), unCommittedVersion);
            finishTS1 = System.nanoTime();
            OrcaVerify.runtimeStatistic.increaseHV13(finishTS1 - startTS1);
          }
        }
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseHV1(finishTS - startTS);
        break;

      case ROLLBACK:
        // 2.3.2如果事务是一个回滚的事务，那么立即从DependencyGraph中移除这个回滚的事务
        startTS = System.nanoTime();
        DependencyGraph.removeProfile(operationTrace.getTransactionID());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseDG2(finishTS - startTS);

      case COMMIT:
        // 2.1.3当事务结束时，使事务profile变得完整
        startTS = System.nanoTime();
        ProfileMap.perfectProfile(
            operationTrace.getTransactionID(),
            operationTrace.getStartTimestamp(),
            operationTrace.getFinishTimestamp(),
            operationTrace.getOperationTraceType());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseProfileMap3(finishTS - startTS);

        // 2.2.2当事务结束时，基于Write set（History Version）中所有未提交Version创建一个最终Version，
        // 包括已提交version和回滚version，
        // 加入到History version和WriteSet(history version)中
        startTS = System.nanoTime();
        HashMap<String, Version> uncommittedVersions =
            WriteSet.getTransactionVersions(
                WriteSet.getHistoryVersion(), operationTrace.getTransactionID());
        if (uncommittedVersions != null) {
          switch (operationTrace.getOperationTraceType()) {
            case COMMIT:
              finalVersionStatus = VersionStatus.COMMITTED;
              break;
            case ROLLBACK:
              finalVersionStatus = VersionStatus.ROLLBACK;
              break;
          }

          for (String key : uncommittedVersions.keySet()) {
            unCommittedVersion = uncommittedVersions.get(key);

            // 2.2.2.1创建final version
            finalVersion =
                new Version(
                    operationTrace.getStartTimestamp(),
                    operationTrace.getFinishTimestamp(),
                    unCommittedVersion,
                    finalVersionStatus);

            // 2.2.2.2将final version加入到write set(history version)中
            WriteSet.addVersion(
                WriteSet.getHistoryVersion(), operationTrace.getTransactionID(), key, finalVersion);

            // 2.2.2.3将final version加入到history version中
            HistoryVersion.addVersion(key, finalVersion);
          }
        }
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseHV2(finishTS - startTS);
        break;
      case DDL:
      case FAULT:
        break;
    }
    // 2.1.4不管是哪种类型的操作都要设置一下事务的ConsistentReadTimeInterval
    ProfileMap.setConsistentReadTimeInterval(operationTrace);

    // 2.4更对对应线程的timestamp line
    // TimestampLine.updateTimestampLine(operationTrace.getThreadID(),
    // operationTrace.getFinishTimestamp());
  }

  /**
   * 获取cursor所指向的trace返回
   *
   * @return cursor指向的trace
   */
  public static OperationTrace peekFromCursor() {
    try {
      return window.get(cursor);
    } catch (IndexOutOfBoundsException e) {
      return null;
    }
  }

  public static OperationTrace peekLastTrace() {
    return window.peekLast();
  }

  /** 将analysis window头部的trace移除，并同步维护相关结构 如果不进行垃圾清理，那么下推游标 */
  public static void removeFromCursor() {

    OperationTrace operationTrace;

    // 0.如果关闭清理机制，那么特殊处理
    if (!Config.LAUNCH_GC) {
      // 0.1获取cursor指向的trace
      operationTrace = peekFromCursor();
      assert operationTrace != null;

      // 0.2维护Write Set(read consistency)，否则验证算法会出错
      switch (operationTrace.getOperationTraceType()) {
        case UPDATE:
        case INSERT:
        case DELETE:
          WriteSet.addTransactionVersions(WriteSet.getReadConsistency(), operationTrace);
          break;
      }

      // 0.3下推游标，并不实际清理Window中的trace
      cursor++;

      return;
    }

    // 1.移除analysis window中的第一条trace
    long startTS = System.nanoTime();
    operationTrace = window.removeFirst();
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAWPrepare(finishTS - startTS);

    // 2.同步维护相关结构
    // 2.1维护服务于读一致性验证的write set
    // 2.2维护当前活跃的事务active
    switch (operationTrace.getOperationTraceType()) {
      case UPDATE:
      case INSERT:
      case DELETE:
        // 2.1.1添加新的version到Write Set(History Version)中
        startTS = System.nanoTime();
        WriteSet.addTransactionVersions(WriteSet.getReadConsistency(), operationTrace);
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.setHV1(finishTS - startTS);
        break;

      case COMMIT:
      case ROLLBACK:
        // 2.1.2当事务结束时，清除该事务占用的Write Set
        startTS = System.nanoTime();
        WriteSet.removeTransactionVersions(
            WriteSet.getReadConsistency(), operationTrace.getTransactionID());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.setHV1(finishTS - startTS);

        // 2.2.1当事务结束时，从active中移除该事务
        startTS = System.nanoTime();
        Active.removeTransaction(operationTrace.getTransactionID());
        finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.setActive1(finishTS - startTS);
        break;

      default:
        break;
    }
  }

  /**
   * 获取从cursor所指向的trace开始的listIterator，包括cursor所指向的trace
   *
   * @return 获取cursor对应的迭代器
   */
  public static ListIterator<OperationTrace> listIterator() {
    return window.listIterator(cursor);
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return 对象自身
   */
  public static Object[] getAllObject() {
    return new Object[] {window};
  }
}
