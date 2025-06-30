package replay.controller.executecontrol;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import replay.controller.ReplayController;
import trace.OperationTrace;
import trace.OperationTraceType;

public class ExecuteFilterMutationOperationDivide extends ExecuteFilterMutationAbs {

  /** 这个mutation会分块地逐步删除case，首先将最大的块作为删除的内容，如果删除失败，把它切分成小块加入队列，进行迭代bfs */
  // 用一个队列存储等待删除的事务
  private final LinkedList<List<String>> operationGroupQueue = new LinkedList<>();

  // 已经被删除的事务
  private final Set<String> reduceOperation = new HashSet<>();

  // 当前准备删除的事务
  private List<String> currentOperationGroup = new ArrayList<>();
  // 切分的数量
  private final int splitNum = 4;

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);

    // 初始化一个空的字符串列表，用于存储所有事务的操作ID
    List<String> operations = new ArrayList<>();
    for (List<List<OperationTrace>> thread : allTxns) {
      for (List<OperationTrace> txn : thread) {
        // 只考虑非开头结尾的操作，因为开头结尾不能删除，否则事务完整性会被破坏
        operations.addAll(
            txn.stream()
                .parallel()
                .filter(this::checkOperationType)
                .map(OperationTrace::getOperationID)
                .collect(Collectors.toList()));
      }
    }

    currentOperationGroup = operations;
    revertMutation();
  }

  public Boolean checkOperationType(OperationTrace operationTrace) {
    OperationTraceType operationTraceType = operationTrace.getOperationTraceType();
    return operationTraceType != OperationTraceType.START
        && operationTraceType != OperationTraceType.COMMIT
        && operationTraceType != OperationTraceType.ROLLBACK;
  }

  /**
   * 按c-reduce的方法判断是否要结束mutation，也就是以下两种情况中的一种 1. 无法继续变异，也就是需要删除的事务数量变小到0 2.
   * 变异失败的次数达到一个阈值，可以通过revert的时候增加计数器来记录
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    return operationGroupQueue.isEmpty();
  }

  /** 把队头取出来，准备变换 */
  @Override
  public void mutation() {
    reduceOperation.addAll(currentOperationGroup);
    // check whether a txn has only two op: begin and end, if so, remove this txn
    for (List<List<OperationTrace>> thread : allTxns_) {
      for (List<OperationTrace> txn : thread) {
        int reducedCnt =
            (int)
                txn.stream()
                    .parallel()
                    .map(OperationTrace::getOperationID)
                    .filter(ReplayController::isExecute)
                    .count();
        if (reducedCnt == 2) {
          reduceOperation.add(txn.get(0).getOperationID());
          reduceOperation.add(txn.get(txn.size() - 1).getOperationID());
        }
      }
    }
    currentOperationGroup = new CopyOnWriteArrayList<>(operationGroupQueue.poll());
  }

  /**
   * 判断输入操作是否包含在变换后的测试案例中 类比mutation transaction来实现
   *
   * @param threadNo 操作所在线程id
   * @param transactionNo 操作所在事务id
   * @param operationId 操作id
   * @return 当前操作是否该被执行
   */
  @Override
  public synchronized boolean isExecute(int threadNo, int transactionNo, String operationId) {
    //        String txnId = operationId.substring(0, operationId.lastIndexOf(","));
    return !(reduceOperation.contains(operationId)) && !currentOperationGroup.contains(operationId);
  }

  /** 回滚最后一次变换，并且把对应的事务组切分成更小的部分，加入队列里 然后把mutation失败的计数器+1 */
  @Override
  public void revertMutation() {
    // 如果只剩一个事务，就不用再切了
    System.out.println("Divide operation group's size is: " + currentOperationGroup.size());
    if (currentOperationGroup.size() == 1) {
      currentOperationGroup.clear();
      return;
    }

    //        List<String> temp = new ArrayList<>(currentOperationGroup);
    //
    //        currentOperationGroup.clear();
    //
    //        currentOperationGroup = temp.stream().parallel().filter(op ->
    // !CascadeDelete.isCascadeDelete(op)).collect(Collectors.toList());
    // 计算每个子列表的基本大小
    int groupSize = currentOperationGroup.size() / splitNum;

    // 记录当前切分的起始索引
    int currentIndex = 0;
    // 计算当前子列表的大小
    int size = (groupSize > 0) ? groupSize : 1;

    // 循环切分列表
    while (currentIndex + size <= currentOperationGroup.size()) {

      // 使用 sublist 方法获取当前子列表
      List<String> subList =
          new ArrayList<>(currentOperationGroup.subList(currentIndex, currentIndex + size));
      operationGroupQueue.add(subList);

      // 更新当前切分的起始索引
      currentIndex += size;
    }
    // 特判最后一组，因为数量可能不齐
    if (currentIndex < currentOperationGroup.size()) {
      operationGroupQueue.add(
          new ArrayList<>(
              currentOperationGroup.subList(currentIndex, currentOperationGroup.size())));
    }

    currentOperationGroup.clear();
    System.out.println("Divide operation group's number is: " + operationGroupQueue.size());
  }

  /**
   * 获取当前变换下，需要删除的操作的 operationId 遍历每个被删除的操作，过一遍isExecute
   *
   * @return 本次被删除的操作
   */
  @Override
  public Set<String> getDeleteOperationTraces() {
    return new HashSet<>(currentOperationGroup);
  }
}
