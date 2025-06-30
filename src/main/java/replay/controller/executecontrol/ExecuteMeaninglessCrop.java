package replay.controller.executecontrol;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import replay.controller.ReplayController;
import trace.OperationTrace;

public class ExecuteMeaninglessCrop extends ExecuteFilterMutationAbs {

  private Set<String> meaninglessOpID;

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);

    meaninglessOpID = new HashSet<>();
  }

  /**
   * 判断是不是已经枚举完所有的情形
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    return true;
  }

  /** 进行一次变换，减少测试案例的规模 */
  @Override
  public void mutation() {
    for (List<List<OperationTrace>> thread : allTxns_) {
      for (List<OperationTrace> txn : thread) {
        // 计算事务里还有多少个操作需要执行
        int cnt =
            (int)
                txn.stream()
                    .parallel()
                    .filter(op -> ReplayController.isExecute(op.getOperationID()))
                    .count();
        // 如果只剩三个以内，说明只剩头尾或者单行事务，这样可以去掉这个事务的头尾 TODO 这里会把ddl都滤掉，但修改的话会有验证问题，暂时没去处理，所以保留
        if (cnt < 3) {
          meaninglessOpID.add(txn.get(0).getOperationID());
          meaninglessOpID.add(txn.get(txn.size() - 1).getOperationID());
        }
      }
    }
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

    return !meaninglessOpID.contains(operationId);
  }

  /** 回滚最后一次变换 */
  @Override
  public void revertMutation() {}

  /**
   * 获取当前变换下，需要删除的操作的 operationId
   *
   * @return 需要删除的 operationId
   */
  @Override
  public Set<String> getDeleteOperationTraces() {
    return null;
  }
}
