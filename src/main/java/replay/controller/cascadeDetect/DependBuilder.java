package replay.controller.cascadeDetect;

import adapter.Adapter;
import gen.operation.enums.OperationType;
import java.util.*;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.TraceUtil;
import util.jdbc.DataSourceUtils;

public class DependBuilder {

  static class TransactionContext {
    String snapshotOpId;
    final IsolationLevel isolationLevel;
    boolean hasRead, hasWrite;

    public TransactionContext(String snapshotOpId, IsolationLevel isolationLevel) {
      this.snapshotOpId = snapshotOpId;
      this.isolationLevel = isolationLevel;
      hasRead = hasWrite = false;
    }
  }

  /**
   * 模拟数据库版本控制，创建依赖图
   *
   * @param chainEdge 版本链
   * @return 操作的依赖图
   */
  public Map<String, Set<String>> build(Map<String, List<OperationTrace>> chainEdge) {
    Map<String, Set<String>> dependMap = new HashMap<>();

    Adapter adapter = DataSourceUtils.getAdapter();
    for (List<OperationTrace> chain : chainEdge.values()) {
      // 初始化辅助的数据结构
      Map<String, String> invisibleVersions = new HashMap<>();
      // 当前在执行的事务所依赖的快照
      Map<String, TransactionContext> currentTransactionContext = new HashMap<>();
      // 维护两种可见的版本操作，一种是当前最新的可见的操作，一种是目前还不可见的操作，都是以opid为标识，key为txnid
      String currentVersion = null;

      // 扫描每个版本链
      for (OperationTrace op : chain) {
        OperationType opType =
            TraceUtil.operationTraceType2OperationType(op.getOperationTraceType());
        assert opType != null;
        // 因为这个操作在这条链第一次出现，更新它的依赖
        dependMap.put(op.getOperationID(), new HashSet<>());
        TransactionContext trxCtx;
        String opId = op.getOperationID();
        String transactionId = op.getTransactionID();

        switch (opType) {
          case START:
            // 如果是开始事务，则初始化事务依赖快照为当前可见的快照
            currentTransactionContext.put(
                transactionId, new TransactionContext(currentVersion, op.getIsolationLevel()));
            break;
          case INSERT:
          case DELETE:
          case UPDATE:
            trxCtx = currentTransactionContext.get(transactionId);
            assert trxCtx != null;
            // 考虑要不要更新快照
            if (adapter.isSnapshotPoint(trxCtx.isolationLevel, opType, false, !trxCtx.hasWrite)) {
              trxCtx.snapshotOpId = currentVersion;
            }
            trxCtx.hasWrite = true;

            // 如果有依赖的快照
            if (trxCtx.snapshotOpId != null) {
              // 更新依赖
              dependMap.get(trxCtx.snapshotOpId).add(opId);
            }

            // 更新事务内版本的更新
            invisibleVersions.put(transactionId, opId);
            // 更新事务内参照的快照
            trxCtx.snapshotOpId = opId;
            break;
          case SELECT:
            trxCtx = currentTransactionContext.get(transactionId);
            assert trxCtx != null;
            // 考虑要不要更新快照
            if (adapter.isSnapshotPoint(trxCtx.isolationLevel, opType, !trxCtx.hasRead, false)) {
              trxCtx.snapshotOpId = currentVersion;
            }
            trxCtx.hasRead = true;

            // 如果有依赖的快照
            if (trxCtx.snapshotOpId != null) {
              // 更新依赖
              dependMap.get(trxCtx.snapshotOpId).add(opId);
            }
            break;
          case COMMIT:
            // 更新当前最新快照，注意处理这个事务没有写这种情况
            currentVersion = invisibleVersions.getOrDefault(transactionId, currentVersion);
            // 清理记录的数据结构
            currentTransactionContext.remove(transactionId);
            invisibleVersions.remove(transactionId);
            break;
          case ROLLBACK:
            // 清理记录的数据结构
            currentTransactionContext.remove(transactionId);
            invisibleVersions.remove(transactionId);
            break;
          default:
            break;
        }
      }
    }

    return dependMap;
  }
}
