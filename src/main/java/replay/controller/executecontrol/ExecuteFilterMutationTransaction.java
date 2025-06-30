package replay.controller.executecontrol;

import static dbloader.transaction.TransactionReplayer.logger;

import java.util.*;
import java.util.stream.Collectors;
import replay.controller.ReplayController;
import trace.OperationTrace;

public class ExecuteFilterMutationTransaction extends ExecuteFilterMutationAbs {
  // 需要执行的事务列表

  protected LinkedList<List<String>> executeTxnQueue;
  protected List<String> currentTxn;
  protected final Set<String> reducedTxnID = new HashSet<>();
  // 返回当前需要删去操作的集合
  protected Set<String> cascade_operation = new HashSet<>();

  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    executeTxnQueue = new LinkedList<>();

    List<String> txnID =
        allTxns_.stream()
            .parallel()
            .flatMap(
                thread ->
                    thread.stream()
                        .filter(txn -> ReplayController.isExecute(txn.get(0).getOperationID()))
                        .map(txn -> txn.get(0).getTransactionID()))
            .collect(Collectors.toList());
    executeTxnQueue.add(txnID);

    currentTxn = new ArrayList<>();
    mutation();
    revertMutation();
  }

  @Override
  public boolean isMutationEnd() {
    return executeTxnQueue.isEmpty();
  }

  @Override
  public void mutation() {
    Set<String> opSet = getReducedOpID();

    reducedTxnID.addAll(currentTxn);
    currentTxn = executeTxnQueue.poll();

    // 如果没删除新的操作，继续迭代
    while (!isMutationEnd() && opSet.containsAll(getDeleteOperationTraces())) {
      reducedTxnID.addAll(currentTxn);
      currentTxn = executeTxnQueue.poll();
    }

    logger.info("Mutation group is: {}", currentTxn);
  }

  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {
    if (operationId == null) {
      return true;
    }
    String threadId = operationId.substring(0, operationId.lastIndexOf(","));
    return !(reducedTxnID.contains(threadId)) && !currentTxn.contains(threadId);
  }

  @Override
  public void revertMutation() {

    if (currentTxn.size() > 1) {
      for (String txnID : currentTxn) {
        List<String> tmp = new ArrayList<>();
        tmp.add(txnID);
        executeTxnQueue.add(tmp);
      }
    }

    currentTxn.clear();
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
                  String threadId = operationId.substring(0, operationId.lastIndexOf(","));
                  return currentTxn.contains(threadId);
                })
            .collect(Collectors.toSet()); // 将过滤后的操作ID收集到Set中

    return cascade_operation;
  }
}
