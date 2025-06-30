package replay.controller.executecontrol;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import trace.OperationTrace;
import trace.OperationTraceType;

public class ExecutionFilterCropRollback extends ExecuteFilterMutationAbs {

  Set<String> rollbackOpID;

  // 返回当前需要删去操作的集合
  final Set<String> cascade_operation = new HashSet<>();

  boolean isEnd = false;

  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    rollbackOpID = new HashSet<>();
    for (List<List<OperationTrace>> thread : allTxns) {
      if (thread.isEmpty() || thread.get(0).isEmpty()) {
        continue;
      }

      for (List<OperationTrace> txn : thread) {
        if (txn.get(txn.size() - 1).getOperationTraceType() == OperationTraceType.ROLLBACK) {
          for (OperationTrace op : txn) {
            rollbackOpID.add(op.getOperationID());
          }
        }
      }
    }
  }

  /** @param rollbackTxn 添加rollback事务 */
  public void addRollback(List<OperationTrace> rollbackTxn) {
    for (OperationTrace op : rollbackTxn) {
      rollbackOpID.add(op.getOperationID());
    }
  }

  /**
   * 判断是不是已经枚举完所有的情形
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    return isEnd;
  }

  /** 进行一次变换，减少测试案例的规模 */
  @Override
  public void mutation() {
    isEnd = true;
  }

  /**
   * 判断输入操作是否包含在变换后的测试案例中
   *
   * @param threadNo 操作所在线程id
   * @param transactionNo 操作所在事务id
   * @param operationId 操作id
   * @return 当前操作是否该被执行
   */
  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    if (operationId == null) {
      return true;
    }
    return !rollbackOpID.contains(operationId);
  }

  /** 回滚最后一次变换 */
  @Override
  public void revertMutation() {
    rollbackOpID.clear();
  }

  /**
   * 返回的是 rollbackTxn 包含的所有 operationTrace
   *
   * @return 包含的所有 operationTrace
   */
  @Override
  public Set<String> getDeleteOperationTraces() {
    cascade_operation.clear();
    cascade_operation.addAll(rollbackOpID);

    return cascade_operation;
  }
}
