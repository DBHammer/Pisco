package replay.controller;

import static context.OrcaContext.configColl;

import ana.main.Config;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import replay.controller.cascadeDetect.CascadeDelete;
import replay.controller.cascadeDetect.VersionOperationGraph;
import replay.controller.executecontrol.ExecuteFilterMutationAbs;
import replay.controller.executecontrol.ExecuteMeaninglessCrop;
import replay.controller.executecontrol.ExecutionFilterCropRollback;
import replay.controller.pausecontrol.serial.OperationSequence;
import replay.controller.pausecontrol.serial.SequenceDependency;
import trace.OperationTrace;
import trace.TraceUtil;

public class ReplayController {
  // 需要执行的操作序列
  @Getter private static OperationSequence operationSequence;

  private static List<ExecuteFilterMutationAbs> executeFilters;

  public static SequenceDependency sequenceDependency;

  @Getter private static List<OperationTrace> errorTraces;

  public static final Map<String, Map.Entry<Integer, Integer>> op2ids = new HashMap<>();

  // 映射txnid和filter表, <thread id, <txn id, txn no>>
  private static Map<Integer, Map<Integer, Integer>> txnIdMappings;
  // 映射thread id和filter表, <thread id, execute thread no>
  private static Map<Integer, Integer> threadIdMappings;

  // 版本链构成的依赖图
  @Getter private static VersionOperationGraph versionOperationGraph;

  @Setter private static ExecuteFilterMutationAbs meaninglessCleaner;

  public static void init() {
    executeFilters = new ArrayList<>();

    errorTraces = new ArrayList<>();

    sequenceDependency = new SequenceDependency();
    sequenceDependency.init();

    txnIdMappings = new HashMap<>();
    threadIdMappings = new HashMap<>();
  }

  public static void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    versionOperationGraph = new VersionOperationGraph(allTxns);

    int threadCnt = 0;
    for (List<List<OperationTrace>> thread : allTxns) {
      int txnCnt = 0;

      int threadId = TraceUtil.getThreadIdFromOpId(thread.get(0).get(0).getOperationID());
      // 真实的thread id 当前线程的序号
      threadIdMappings.put(threadId, threadCnt);
      Map<Integer, Integer> txnMappings = new HashMap<>();
      for (List<OperationTrace> txn : thread) {
        int txnId = TraceUtil.getTxnIdFromOpId(txn.get(0).getOperationID());
        txnMappings.put(txnId, txnCnt);
        txnCnt++;

        op2ids.putAll(
            txn.stream()
                .parallel()
                .collect(
                    Collectors.toMap(
                        OperationTrace::getOperationID, op -> Map.entry(threadId, txnId))));
      }
      txnIdMappings.put(threadId, txnMappings);
      threadCnt++;
    }
  }

  public static void addExecuteFilter(ExecuteFilterMutationAbs filter) {
    executeFilters.add(filter);
  }

  public static void initOperationSequence() {
    if (configColl.getReplay().getOperationSequence() != null) {
      operationSequence =
          new OperationSequence(
              Config.NUMBER_THREAD, configColl.getReplay().parseOperationSequence());
      return;
    }
    operationSequence =
        new OperationSequence(Config.NUMBER_THREAD, sequenceDependency.generateOperationSequence());
    //        sequenceDependency.pinData();
    //        for (int i = 5000; i < 25000; i += 5000) {
    //            if (!sequenceDependency.setOperationListSize(i)){
    //                break;
    //            }
    ////
    //            long startTime = System.currentTimeMillis();
    //            operationSequence = new
    // OperationSequence(Config.NUMBER_THREAD,sequenceDependency.generateOperationSequenceNaive());
    //            System.out.println("[initOperationSequenceNaive] " + i + " : " +
    // (System.currentTimeMillis() - startTime));
    //
    //            if (!sequenceDependency.setOperationListSize(i)){
    //                break;
    //            }
    //
    //            startTime = System.currentTimeMillis();
    //
    //            System.out.println("[initOperationSequence] " + i + " : " +
    // (System.currentTimeMillis() - startTime));
    //        }

    //        System.exit(0);

    //        System.out.println("[initOperationSequence] " +
    // operationSequence.getOperationSeq().toString());
    if (configColl.getReplay().isFlatten()) {
      operationSequence.flatten();
    }
  }

  public static void initVersionGraph(List<List<List<OperationTrace>>> allTxn) {
    versionOperationGraph = new VersionOperationGraph(allTxn);
  }

  public static Set<String> getTailOperationSet(Set<OperationTrace> errorTraces) {
    return operationSequence.getTailOpId(errorTraces);
  }

  public static void addErrorTrace(OperationTrace trace) {
    errorTraces.add(trace);
  }

  public static void reset() {
    sequenceDependency.init();
    operationSequence.reset();
  }

  public static boolean isExecute(String operationId) {
    // 如果被级联删除了，提前判并终止
    if (CascadeDelete.isCascadeDelete(operationId)) {
      return false;
    }
    int threadId = op2ids.get(operationId).getKey();
    int txnId = op2ids.get(operationId).getValue();

    int threadNo = threadIdMappings.get(threadId);
    int txnNo = txnIdMappings.get(threadId).get(txnId);
    return isExecute(threadNo, txnNo, operationId);
  }

  public static boolean isExecute(int threadNo, int transactionNo, String operationId) {
    // 挨个判断该不该执行，只要有一个filter认为不该执行，就不执行，这样可以避免合并不同逻辑的filter
    for (ExecuteFilterMutationAbs filter : executeFilters) {
      if (!filter.isExecute(threadNo, transactionNo, operationId)) return false;
    }
    return true;
  }

  public static void isPause(OperationTrace operationTrace) {
    String operationId = operationTrace.getOperationID();

    if (configColl.getReplay().isSerial()) {
      operationSequence.execute(operationId);
    }
  }

  public static void addFinishOp(String operationId) {
    operationSequence.executeFinish(operationId);
  }

  public static synchronized void addRollbackTxn(List<OperationTrace> rollbackTxn) {

    for (ExecuteFilterMutationAbs filter : executeFilters) {
      if (filter instanceof ExecutionFilterCropRollback) {
        ((ExecutionFilterCropRollback) filter).addRollback(rollbackTxn);
        return;
      }
    }
  }

  public static void printFinalSequence() {
    OperationSequence finalSequence = new OperationSequence(operationSequence);
    finalSequence.filterExecuteOp();
    finalSequence.print();
  }

  public static void moveCursor() {
    operationSequence.moveCursor();
  }

  public static String getLastFilterClassName() {
    return executeFilters.get(executeFilters.size() - 1).getClass().getName();
  }

  // 去除无意义的操作
  public static void cropMeaninglessOp() {
    for (ExecuteFilterMutationAbs filter : executeFilters) {
      if (filter instanceof ExecuteMeaninglessCrop) {
        filter.mutation();
        return;
      }
    }
  }

  public static void cleanMeaningless() {
    if (meaninglessCleaner != null) {
      meaninglessCleaner.mutation();
    }
  }
}
