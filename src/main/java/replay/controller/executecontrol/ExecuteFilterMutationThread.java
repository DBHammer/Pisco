package replay.controller.executecontrol;

import static dbloader.transaction.TransactionReplayer.logger;

import java.util.*;
import java.util.stream.Collectors;
import trace.OperationTrace;

public class ExecuteFilterMutationThread extends ExecuteFilterMutationAbs {
  // 需要执行的事务列表

  protected LinkedList<List<String>> executeThreadQueue;
  protected List<String> currentThread = new ArrayList<>();
  protected final Set<String> reducedThreadID = new HashSet<>();
  // 返回当前需要删去操作的集合
  protected Set<String> cascade_operation = new HashSet<>();

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    executeThreadQueue = new LinkedList<>();
    allTxns.stream()
        .parallel()
        .filter(thread -> thread != null && !thread.isEmpty() && !thread.get(0).isEmpty())
        .forEach(
            thread -> {
              String threadId = thread.get(0).get(0).getThreadID();
              executeThreadQueue.add(new ArrayList<>(Collections.singletonList(threadId)));
            });

    mutation();
    revertMutation();
  }

  @Override
  public boolean isMutationEnd() {
    return executeThreadQueue.isEmpty();
  }

  @Override
  public void mutation() {
    Set<String> opSet = getReducedOpID();

    reducedThreadID.addAll(currentThread);
    currentThread = executeThreadQueue.poll();

    // 如果没删除新的操作，继续迭代
    while (!isMutationEnd() && opSet.containsAll(getDeleteOperationTraces())) {
      reducedThreadID.addAll(currentThread);
      currentThread = executeThreadQueue.poll();
    }
    logger.info("Mutation group is: {}", currentThread);
  }

  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    if (operationId == null || currentThread == null) {
      return true;
    }
    String threadId = operationId.substring(0, operationId.indexOf(","));

    return !(reducedThreadID.contains(threadId)) && !currentThread.contains(threadId);
  }

  @Override
  public void revertMutation() {
    currentThread.clear();
  }

  @Override
  public Set<String> getDeleteOperationTraces() {
    cascade_operation =
        allTxns_.stream() // 将所有的线程事务转换为流式计算
            .flatMap(
                txnGroup ->
                    txnGroup.stream() // 将每个线程转换为流式计算
                        .filter(txn -> !txn.isEmpty()) // 过滤掉空的线程
                        .flatMap(txn -> txn.stream().map(OperationTrace::getOperationID)))
            .filter(
                operationId -> { // 把操作id转化为线程id
                  String threadId = operationId.substring(0, operationId.indexOf(","));
                  return currentThread != null && currentThread.contains(threadId);
                })
            .collect(Collectors.toSet()); // 将过滤后的操作ID收集到Set中

    return cascade_operation;
  }
}
