package replay.controller.executecontrol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import replay.controller.ReplayController;
import trace.OperationTrace;

public abstract class ExecuteFilterMutationAbs {

  public List<List<List<OperationTrace>>> allTxns_;

  /**
   * 判断是不是已经枚举完所有的情形
   *
   * @return true if all the mutation is ended
   */
  public abstract boolean isMutationEnd();

  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    allTxns_ = new ArrayList<>();
    allTxns_.addAll(allTxns);
  }

  /** 进行一次变换，减少测试案例的规模 */
  public abstract void mutation();

  /**
   * 判断输入操作是否包含在变换后的测试案例中
   *
   * @param threadNo 操作所在线程id
   * @param transactionNo 操作所在事务id
   * @param operationId 操作id
   * @return 当前操作是否该被执行
   */
  public abstract boolean isExecute(int threadNo, int transactionNo, String operationId);

  /** 回滚最后一次变换 */
  public abstract void revertMutation();

  /**
   * 获取当前变换下，需要删除的操作的 operationId
   *
   * @return 需要删除的操作的 operationId
   */
  public abstract Set<String> getDeleteOperationTraces();

  protected Set<String> getReducedOpID() {
    Set<String> opSet = new HashSet<>();

    for (List<List<OperationTrace>> thread : allTxns_) {
      for (List<OperationTrace> txn : thread) {
        for (OperationTrace op : txn) {
          if (!ReplayController.isExecute(op.getOperationID())) {
            opSet.add(op.getOperationID());
          }
        }
      }
    }

    return opSet;
  }
}
