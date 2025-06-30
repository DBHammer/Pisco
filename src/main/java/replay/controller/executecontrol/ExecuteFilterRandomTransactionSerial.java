package replay.controller.executecontrol;

import context.OrcaContext;
import java.util.*;
import trace.OperationTrace;

public class ExecuteFilterRandomTransactionSerial extends ExecuteFilterMutationAbs {

  // 需要执行的事务列表
  private List<List<Integer>> executeTxn; // 1: will be executed, 0: will not be executed

  // 阈值 (可调)
  public static final int THRESHOLD = 10;

  /**
   * 维护一个还未执行的线程组的集合，每次一个重新开始的 mutation 将从这个集合中随机选取一个线程 threadId 执行 维护一个滑动窗口，初始大小为事务数量 size -
   * 1，根据是否成功删除判断下一个窗口大小 - 如果删除成功，窗口大小 = remainder size - 1 - 如果删除失败，窗口大小折半 事务起点：random of
   * (remainder size - slideWindow) 关于阈值的设置：每个线程维护一个阈值，如果当前线程失败的次数已经超过阈值，则立刻进入下一个 threadId
   */
  private Integer threadId;

  private Integer transactionLeft;
  private Integer slideWindow;
  private Boolean currentThreadFinish;
  private Boolean lastMutationResult;
  private Integer remainderSize;
  private Integer failCount;
  private List<Integer> threadList;

  private List<Integer> transactionList;

  // 返回当前需要删去操作的集合
  final Set<String> cascadeOperation = new HashSet<>();

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    executeTxn = new ArrayList<>();
    threadList = new ArrayList<>();
    for (int i = 0; i < OrcaContext.configColl.getLoader().getNumberOfLoader(); i++) {
      ArrayList<Integer> list = new ArrayList<>();
      threadList.add(i);
      for (int j = 0; j < OrcaContext.configColl.getLoader().getExecCountPerLoader(); j++) {
        // 表示初始的时候都是要执行的
        list.add(1);
      }
      executeTxn.add(list);
    }
    currentThreadFinish = true;
    failCount = 0;
    slideWindow = 0;
    transactionList = new ArrayList<>();
  }

  /**
   * 按c-reduce的方法判断是否要结束mutation，也就是以下两种情况中的一种 1. 无法继续变异，也就是需要删除的事务数量变小到0 2.
   * 变异失败的次数达到一个阈值，可以通过revert的时候增加计数器来记录
   *
   * @return true if all the mutation is ended
   */
  @Override
  public boolean isMutationEnd() {
    if (threadList.isEmpty() && (slideWindow <= 1 || remainderSize == 0)) {
      return true;
    }
    return false;
  }

  /**
   * 进行一次变换，减少测试案例的规模 1. 随机选择一个线程 2. 随机选择一个事务作为需要删除的事务的开始位置 3.
   * 随机选择需要删除的事务数量，这个数量一开始可以比较大，然后随着mutation逐渐变小 保证每次mutation有新删除至少一个事务，否则需要减小删除的事务数量，重新mutation
   */
  @Override
  public void mutation() {
    if (currentThreadFinish) {
      int randomIndex = new Random(System.currentTimeMillis()).nextInt(threadList.size());
      threadId = threadList.get(randomIndex);
      threadList.remove(randomIndex);
      currentThreadFinish = false;
      slideWindow = allTxns_.get(threadId).size() - 1;
      remainderSize = allTxns_.get(threadId).size();
      failCount = 0;
    } else {
      if (!lastMutationResult) {
        slideWindow = slideWindow / 2;
      }
      slideWindow = Math.min(slideWindow, remainderSize);
    }
    lastMutationResult = true;

    // 这次删的可能包含了上次删除的，优化为了找不相交的事务块
    transactionLeft =
        new Random(System.currentTimeMillis()).nextInt(remainderSize - slideWindow + 1);
    int i = transactionLeft;
    int count = 0;
    transactionList.clear();
    while (count < slideWindow) {
      if (isExecute(threadId, i, null)) {
        executeTxn.get(threadId).set(i, 0);
        transactionList.add(i);
        count++;
      }
      i++;
      if (i >= allTxns_.get(threadId).size()) {
        i = 0;
      }
    }

    remainderSize -= slideWindow;

    if (slideWindow == 1 || remainderSize == 0) {
      currentThreadFinish = true;
    }
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
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    return executeTxn.get(threadNo).get(transactionNo) == 1;
  }

  /** 回滚最后一次变换 然后把mutation失败的计数器+1 */
  @Override
  public void revertMutation() {
    for (Integer index : transactionList) {
      executeTxn.get(threadId).set(index, 1);
    }
    remainderSize += slideWindow;
    lastMutationResult = false;
    failCount++;
    if (failCount >= THRESHOLD) {
      currentThreadFinish = true;
    }
  }

  /**
   * 获取当前变换下，需要删除的操作的 operationId 遍历每个被删除的操作，过一遍isExecute
   *
   * @return 本次变换会删除的操作
   */
  @Override
  public Set<String> getDeleteOperationTraces() {
    cascadeOperation.clear();
    for (Integer index : transactionList) {
      if (executeTxn.get(threadId).get(index) == 0) {
        List<OperationTrace> operationTraces = allTxns_.get(threadId).get(index);
        for (OperationTrace operationTrace : operationTraces) {
          cascadeOperation.add(operationTrace.getOperationID());
        }
      }
    }
    return cascadeOperation;
  }
}
