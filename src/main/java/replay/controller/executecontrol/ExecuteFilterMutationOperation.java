package replay.controller.executecontrol;

import java.util.*;
import replay.controller.ReplayController;
import trace.OperationTrace;
import trace.OperationTraceType;

public class ExecuteFilterMutationOperation extends ExecuteFilterMutationAbs {
  // 记录每个操作是否要执行
  private Map<String, Boolean> operationMap; // < operationId, operationCursor > 映射关系

  // 将所有能精简的操作有序排列成的列表，用于进行枚举
  private List<String> operations; // < operation

  // 当前枚举的进度，与operations对应
  private int operationCursor;

  // 返回当前需要删去操作的集合
  final Set<String> cascadeOperation = new HashSet<>();

  // 所有线程的所有操作全部初始化：都允许执行
  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    operationMap = new HashMap<>();
    operations = new ArrayList<>();
    for (List<List<OperationTrace>> thread : allTxns) {
      for (List<OperationTrace> txn : thread) {
        // 只考虑非开头结尾的操作，因为开头结尾不能删除，否则事务完整性会被破坏
        txn.stream()
            .parallel()
            .filter(this::checkOperationType)
            .map(OperationTrace::getOperationID)
            .filter(ReplayController::isExecute)
            .forEach(
                op -> {
                  operationMap.put(op, true);
                  operations.add(op);
                });
      }
    }

    // 初始化指针
    operationCursor = -1;
  }

  // 判断是不是所有的操作都试着去掉过了
  @Override
  public boolean isMutationEnd() {
    return operationCursor >= operations.size();
  }

  // 依次尝试去掉一个操作：用指针维护
  @Override
  public void mutation() {
    // 1.initial
    operationCursor++;

    // 2. 当操作已经被去除时跳过
    while (!isMutationEnd() && !ReplayController.isExecute(operations.get(operationCursor))) {
      operationMap.put(operations.get(operationCursor), false);
      operationCursor++;
    }

    if (!isMutationEnd()) {
      operationMap.put(operations.get(operationCursor), false);
    }
  }

  // 判断当前的操作该不该执行
  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    // 两种情况，一种是这个操作不可遍历（是事务的开头结尾），一种是保留
    if (operationId == null) {
      return true;
    }
    return !operationMap.containsKey(operationId) || operationMap.get(operationId);
  }

  // 如果发现去掉操作之后不能复现，那就回滚这个操作，表示它应该被执行
  @Override
  public void revertMutation() {
    if (!isMutationEnd()) {
      operationMap.put(operations.get(operationCursor), true);
    }
  }

  @Override
  public Set<String> getDeleteOperationTraces() {
    cascadeOperation.clear();
    if (operationCursor < operations.size()) {
      cascadeOperation.add(operations.get(operationCursor));
    }
    return cascadeOperation;
  }

  public Boolean checkOperationType(OperationTrace operationTrace) {
    OperationTraceType operationTraceType = operationTrace.getOperationTraceType();
    return operationTraceType != OperationTraceType.START
        && operationTraceType != OperationTraceType.COMMIT
        && operationTraceType != OperationTraceType.ROLLBACK;
  }
}
