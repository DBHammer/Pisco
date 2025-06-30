package replay.controller.executecontrol;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import trace.OperationTrace;

public class ExecuteFilterDivideReduce extends ExecuteFilterMutationAbs {

  /** 这个mutation会分块地逐步删除case，首先将最大的块作为删除的内容，如果删除失败，把它切分成小块加入队列，进行迭代bfs */
  // 用一个队列存储等待删除的事务
  private final LinkedList<List<String>> txnGroupQueue = new LinkedList<>();

  // 已经被删除的事务
  private final Set<String> reduceTxn = new HashSet<>();

  // 当前准备删除的事务
  private List<String> currentTxnGroup = new ArrayList<>();
  // 切分的数量
  private final int splitNum = 4;

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    allTxns_ = new ArrayList<>();
    allTxns_.addAll(allTxns);

    // 初始化一个空的字符串列表，用于存储所有事务的第一个元素的事务ID
    List<String> initGroup =
        allTxns_.stream()
            .parallel()
            // 将所有事务组合到一个单独的流中
            .flatMap(
                txnGroup ->
                    txnGroup.stream()
                        // 过滤空的事务列表
                        .filter(txn -> !txn.isEmpty())
                        // 提取每个事务列表的第一个元素的事务ID
                        .map(txn -> txn.get(0).getTransactionID()))
            // 将提取出的事务ID收集到一个列表中
            .collect(Collectors.toList());

    txnGroupQueue.add(initGroup);
  }

  /**
   * 按c-reduce的方法判断是否要结束mutation，也就是以下两种情况中的一种 1. 无法继续变异，也就是需要删除的事务数量变小到0 2.
   * 变异失败的次数达到一个阈值，可以通过revert的时候增加计数器来记录
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    return txnGroupQueue.isEmpty();
  }

  /** 把队头取出来，准备变换 */
  @Override
  public void mutation() {
    reduceTxn.addAll(currentTxnGroup);
    currentTxnGroup = new CopyOnWriteArrayList<>(txnGroupQueue.poll());
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
    String txnId = operationId.substring(0, operationId.lastIndexOf(","));
    return !(reduceTxn.contains(txnId)) && !currentTxnGroup.contains(txnId);
  }

  /** 回滚最后一次变换，并且把对应的事务组切分成更小的部分，加入队列里 然后把mutation失败的计数器+1 */
  @Override
  public void revertMutation() {
    // 如果只剩一个事务，就不用再切了
    if (currentTxnGroup.size() == 1) {
      currentTxnGroup.clear();
      return;
    }
    // 计算每个子列表的基本大小
    int groupSize = currentTxnGroup.size() / 2;

    // 记录当前切分的起始索引
    int currentIndex = 0;
    // 计算当前子列表的大小
    int size = groupSize > 0 ? groupSize : 1;

    // 循环切分列表
    while (currentIndex + size <= currentTxnGroup.size()) {

      // 使用 sublist 方法获取当前子列表
      List<String> subList =
          new ArrayList<>(currentTxnGroup.subList(currentIndex, currentIndex + size));
      txnGroupQueue.add(subList);

      // 更新当前切分的起始索引
      currentIndex += size;
    }
    // 特判最后一组，因为数量可能不齐
    if (currentIndex < currentTxnGroup.size()) {
      txnGroupQueue.add(
          new ArrayList<>(currentTxnGroup.subList(currentIndex, currentTxnGroup.size())));
    }

    currentTxnGroup.clear();
  }

  /**
   * 获取当前变换下，需要删除的操作的 operationId 遍历每个被删除的操作，过一遍isExecute
   *
   * @return 本次被删除的操作
   */
  @Override
  public Set<String> getDeleteOperationTraces() {

    return allTxns_.stream() // 将所有的线程事务转换为流式计算
        .flatMap(
            txnGroup ->
                txnGroup.stream() // 将每个线程事务转换为流式计算
                    .filter(txn -> !txn.isEmpty()) // 过滤掉空的事务
                    .flatMap(
                        txn ->
                            txn.stream()
                                .map(OperationTrace::getOperationID))) // 将每个事务的所有操作ID转换为流式计算
        .filter(
            operationId -> { // 把操作id转化为事务id
              String txnId = operationId.substring(0, operationId.lastIndexOf(","));
              return currentTxnGroup.contains(txnId);
            })
        .collect(Collectors.toSet());
  }
}
