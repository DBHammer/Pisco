package replay.controller.executecontrol;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import replay.controller.ReplayController;
import trace.OperationTrace;

public class ExecutionFilterCropTailTxn extends ExecuteFilterMutationAbs {

  private Set<String> tailOpID = null;

  // 返回当前需要删去操作的集合
  final Set<String> cascade_operation = new HashSet<>();

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    mutation();
  }

  /**
   * 判断是不是已经枚举完所有的情形 在初始化的时候已经枚举完了
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    return tailOpID != null;
  }

  /** 进行一次变换，减少测试案例的规模 */
  @Override
  public void mutation() {
    Set<OperationTrace> errorTrace = new HashSet<>(ReplayController.getErrorTraces());
    // 获得最后一个错误trace执行后的操作id
    tailOpID = ReplayController.getTailOperationSet(errorTrace);
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
    //    boolean isExecuted = false;
    //    try{
    //      isExecuted = !tailOpID.contains(operationId);
    //    }
    //    catch (Exception e){
    //      System.out.println();
    //    }

    return !tailOpID.contains(operationId);
  }

  /** 回滚最后一次变换 */
  @Override
  public void revertMutation() {
    tailOpID = null;
  }

  @Override
  public Set<String> getDeleteOperationTraces() {
    cascade_operation.clear();
    return cascade_operation;
  }
}
