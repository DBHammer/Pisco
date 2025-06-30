package replay.controller.cascadeDetect;

import adapter.Adapter;
import gen.operation.enums.OperationType;
import java.util.*;
import java.util.stream.Collectors;
import replay.controller.ReplayController;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.TraceUtil;
import util.jdbc.DataSourceUtils;

public class DataChainBuilder {

  // 维护当前未结束的事务上下文
  private List<OperationTrace> cachedTransaction = new ArrayList<>();
  // 快速寻找cachedTransaction中操作的辅助结构，key是txnId
  private final Map<String, List<OperationTrace>> cachedOperations = new HashMap<>();
  // 记录每个事务的隔离级别，key是txnId
  private final Map<String, IsolationLevel> txnIsolationLevel = new HashMap<>();

  public Map<String, List<OperationTrace>> build(Map<String, OperationTrace> operationTraceMap) {
    Map<String, List<OperationTrace>> versionChain = new HashMap<>();

    List<List<String>> operationSequence =
        ReplayController.getOperationSequence().getOperationSeq();

    // 按顺序扫描序列
    for (List<String> operations : operationSequence) {

      for (String op : operations) {

        OperationTrace operationTrace = operationTraceMap.get(op);

        if (operationTrace == null) {
          continue;
        }

        // 按访问的数据分解进不同的链里面
        Set<String> data = TraceUtil.getDataSet(operationTrace);

        for (String key : data) {
          // 有的话直接加进链里
          if (versionChain.containsKey(key)) {
            versionChain.get(key).add(operationTrace);
          } else {
            // 没有的话新建对应的版本链，然后把前面可能涉及的事务上下文加入进去
            // TODO 会额外加入一些可能没有访问当前数据的事务操作，后续最好需要把他们去掉
            List<OperationTrace> newChain = new ArrayList<>(cachedTransaction);
            versionChain.put(key, newChain);
          }
        }

        // 如果是事务快照和结束的操作，也把它放进所有的版本链里
        // 需要避免重复添加
        OperationType opType =
            TraceUtil.operationTraceType2OperationType(operationTrace.getOperationTraceType());
        if (opType == OperationType.START
            || opType == OperationType.COMMIT
            || opType == OperationType.ROLLBACK
            || isSnapshot(operationTrace)) {
          for (String key : versionChain.keySet()) {
            if (!data.contains(key)) {
              versionChain.get(key).add(operationTrace);
            }
          }

          // 同时记录到现在未提交的事务起止和快照获取操作
          cacheTransactionContext(operationTrace);
        }
      }
    }

    return versionChain;
  }

  /**
   * 判断操作是否获取快照
   *
   * @param operation 需要判断的操作
   * @return 获取快照
   */
  private boolean isSnapshot(OperationTrace operation) {
    Adapter adapter = DataSourceUtils.getAdapter();

    OperationType opType =
        TraceUtil.operationTraceType2OperationType(operation.getOperationTraceType());
    String txnId = operation.getTransactionID();
    // 是不是第一个写操作
    boolean isFirstWriteOp =
        TraceUtil.isWriteOperation(operation)
            && cachedOperations.get(txnId).stream().anyMatch(TraceUtil::isWriteOperation);
    // 是不是第一个读操作
    boolean isFirstReadOp =
        (opType == OperationType.SELECT
            && cachedOperations.get(txnId).stream().anyMatch(TraceUtil::isReadOperation));

    return adapter.isSnapshotPoint(
        txnIsolationLevel.get(txnId), opType, isFirstReadOp, isFirstWriteOp);
  }

  /**
   * 维护未提交的事务上下文
   *
   * @param operation 新加入的事务
   */
  private void cacheTransactionContext(OperationTrace operation) {

    OperationType opType =
        TraceUtil.operationTraceType2OperationType(operation.getOperationTraceType());
    String txnId = operation.getTransactionID();

    if (opType == OperationType.START) {
      cachedOperations.put(txnId, new ArrayList<>());
      txnIsolationLevel.put(txnId, operation.getIsolationLevel());
    }

    // 只用记录和快照相关的上下文
    if (isSnapshot(operation)) {
      cachedTransaction.add(operation);
      cachedOperations.get(txnId).add(operation);
    }

    // 事务结束，则清理维护的上下文
    if (opType == OperationType.COMMIT || opType == OperationType.ROLLBACK) {
      cachedTransaction =
          cachedTransaction.stream()
              .filter(op -> !op.getTransactionID().equals(txnId))
              .collect(Collectors.toList());
      cachedOperations.remove(txnId);
    }
  }
}
