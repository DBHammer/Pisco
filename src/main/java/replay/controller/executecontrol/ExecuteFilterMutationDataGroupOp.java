package replay.controller.executecontrol;

import java.util.*;
import trace.OperationTrace;

/** 本算子/过滤器基于事务访问的数据进行筛选 在初始化阶段，initializeWithAllTxn将操作按访问的数据分组，以组为单位去掉操作 */
public class ExecuteFilterMutationDataGroupOp extends ExecuteFilterMutationAbs {

  // opid -> key
  private Map<String, Set<String>> op2keys;

  private List<String> keyList;

  // 记录每组事务需不需要执行
  private List<Boolean> groupExecutionFlag; // thread, trx, operation, 0/1 代表是否可以执行

  private int groupCursor = -1; // 实际维护的是分组的数量

  // 返回当前需要删去操作的集合
  final Set<String> cascade_operation = new HashSet<>();

  // 所有操作初始化：都允许执行
  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);

    keyList = new ArrayList<>();
    op2keys = new HashMap<>();

    for (List<List<OperationTrace>> thread : allTxns) {
      for (List<OperationTrace> trx : thread) {
        for (OperationTrace operationTrace : trx) {
          Set<String> keys = new HashSet<>();

          if (operationTrace.getReadTupleList() != null) {
            operationTrace.getReadTupleList().forEach(trace -> keys.add(trace.getPrimaryKey()));
          }
          if (operationTrace.getWriteTupleList() != null
              && operationTrace.getWriteTupleList().size() == 1) {
            operationTrace.getWriteTupleList().forEach(trace -> keys.add(trace.getPrimaryKey()));
          }

          for (String key : keys) {
            if (!keyList.contains(key)) {
              keyList.add(key);
            }
          }

          op2keys.put(operationTrace.getOperationID(), keys);
        }
      }
    }

    // 初始化flag
    // 根据dataSet的大小把groupExecutionFlag全部初始化为true
    groupExecutionFlag = new ArrayList<>(Collections.nCopies(keyList.size(), true));
  }

  // 判断是不是所有的操作都试着去掉过了
  @Override
  public boolean isMutationEnd() {
    return groupCursor >= groupExecutionFlag.size();
  }

  // 依次尝试去掉一个操作：用指针维护
  @Override
  public void mutation() {
    Set<String> opSet = getReducedOpID();

    groupCursor++;
    if (isMutationEnd()) return;

    groupExecutionFlag.set(groupCursor, false);

    // 如果没有减少数量，继续迭代
    while (!isMutationEnd() && opSet.containsAll(getDeleteOperationTraces())) {
      groupCursor++;
      groupExecutionFlag.set(groupCursor, false);
    }
  }

  // 判断当前的事务该不该执行，只要事务访问的一个数据项对应的组不执行，这个事务就不执行
  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    if (operationId == null) {
      return true;
    }

    Set<String> keys = op2keys.get(operationId);
    for (String key : keys) {
      int idx = keyList.indexOf(key);
      if (!groupExecutionFlag.get(idx)) {
        return false;
      }
    }
    return true;
  }

  // 如果发现去掉操作之后不能复现，那就回滚这个操作，表示它应该被执行
  @Override
  public void revertMutation() {
    groupExecutionFlag.set(groupCursor, true);
  }

  @Override
  public Set<String> getDeleteOperationTraces() {
    cascade_operation.clear();
    for (List<List<OperationTrace>> lists : allTxns_) {
      for (List<OperationTrace> list : lists) {
        for (OperationTrace op : list) {
          Set<String> keys = op2keys.get(op.getOperationID());
          for (String key : keys) {
            int idx = keyList.indexOf(key);
            if (idx == groupCursor) {
              cascade_operation.add(op.getOperationID());
              break;
            }
          }
        }
      }
    }

    return cascade_operation;
  }
}
