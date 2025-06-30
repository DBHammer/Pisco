package replay.controller.executecontrol;

import java.util.*;
import trace.OperationTrace;

/**
 * 本算子/过滤器基于事务访问的数据进行筛选 在初始化阶段，initializeWithAllTxn将事务按访问的数据分组，以组为单位去掉事务
 * 注意一个事务会访问多个数据，所以同一个事务可能出现在多个组里
 */
public class ExecuteFilterMutationDataGroupDivide extends ExecuteFilterMutationAbs {

  // txn no 到group的映射，便于查找事务是否被去掉，具体是<thread no,txn no, set{group no}>的结构
  private Map<Integer, Map<Integer, Set<Integer>>> txn2Group;

  private LinkedList<List<Integer>> executeDataGroupQueue;
  private List<Integer> currentDataGroup;
  private final Set<Integer> reducedDataGroup = new HashSet<>();

  // 返回当前需要删去操作的集合
  final Set<String> cascade_operation = new HashSet<>();

  // 所有操作初始化：都允许执行
  @Override
  public void initializeWithAllTxn(List<List<List<OperationTrace>>> allTxns) {
    super.initializeWithAllTxn(allTxns);
    // 构造txnGroup和txn2Group

    // 事务访问的数据记录在trace的writeTupleList和readTupleList里的PrimaryKey（具体看操作是读操作还是写操作

    // 数据的集合，datum 维护的 dataNo 分别是数据的编号
    List<String> dataSet = new ArrayList<>();
    txn2Group = new HashMap<>();

    // 在这遍历所有的trace，把里面的数据提取出来
    int threadCnt = 0;
    for (List<List<OperationTrace>> thread : allTxns) {
      int txnCnt = 0;
      Map<Integer, Set<Integer>> txns = new HashMap<>(); // txn, 每个组包含数据的编号集合
      for (List<OperationTrace> transaction : thread) {
        Set<String> dataInTxn = new HashSet<>();
        // 在这里把所有的数据都放进dataInTxn里
        for (OperationTrace operationTrace : transaction) {
          if (operationTrace.getReadTupleList() != null) {
            operationTrace
                .getReadTupleList()
                .forEach(trace -> dataInTxn.add(trace.getPrimaryKey()));
          }
          if (operationTrace.getWriteTupleList() != null
              && operationTrace.getWriteTupleList().size() == 1) {
            operationTrace
                .getWriteTupleList()
                .forEach(trace -> dataInTxn.add(trace.getPrimaryKey()));
          }
        }

        // 然后把数据转换为编号记录下来
        Set<Integer> datum = new HashSet<>(); // 一个 datum 代表一个事务所有操作涉及到的数据
        for (String data : dataInTxn) {
          if (!dataSet.contains(data)) { // 如果是一个未知的数据
            dataSet.add(data);
          }

          int dataNo = dataSet.indexOf(data);
          datum.add(dataNo);
        }
        txns.put(txnCnt, datum);
        txnCnt++;
      }

      txn2Group.put(threadCnt, txns);
      threadCnt++;
    }

    executeDataGroupQueue = new LinkedList<>();
    List<Integer> initDataGroup = new ArrayList<>();
    for (int i = 0; i < dataSet.size(); i++) {
      initDataGroup.add(i);
    }
    executeDataGroupQueue.add(initDataGroup);

    currentDataGroup = new ArrayList<>();
    mutation();
    revertMutation();
  }

  // 判断是不是所有的操作都试着去掉过了
  @Override
  public boolean isMutationEnd() {
    return executeDataGroupQueue.isEmpty();
  }

  // 依次尝试去掉一个操作：用指针维护
  @Override
  public void mutation() {
    Set<String> opSet = getReducedOpID();

    reducedDataGroup.addAll(currentDataGroup);
    currentDataGroup = executeDataGroupQueue.poll();

    // 如果没删除新的操作，继续迭代
    while (!isMutationEnd()
        && (opSet.containsAll(getDeleteOperationTraces()) || currentDataGroup.size() == 1)) {
      reducedDataGroup.addAll(currentDataGroup);
      currentDataGroup = executeDataGroupQueue.poll();
    }
    System.out.println("Mutation group is: " + currentDataGroup);
  }

  // 判断当前的事务该不该执行，只要事务访问的一个数据项对应的组不执行，这个事务就不执行
  @Override
  public boolean isExecute(int threadNo, int transactionNo, String operationId) {

    Set<Integer> dataSet = txn2Group.get(threadNo).get(transactionNo);
    for (int dataNo : dataSet) {
      if (reducedDataGroup.contains(dataNo) || currentDataGroup.contains(dataNo)) {
        return false;
      }
    }
    return true;

    //        return txn2Group.get(threadNo).get(transactionNo).stream().parallel()
    //                .noneMatch(dataNo -> (reducedDataGroup.contains(dataNo) ||
    // currentDataGroup.contains(dataNo)));
  }

  // 如果发现去掉操作之后不能复现，那就回滚这个操作，表示它应该被执行
  @Override
  public void revertMutation() {
    // 如果只剩一个线程，就不用再切了
    if (currentDataGroup.size() == 1) {
      currentDataGroup.clear();
      return;
    }
    // 计算每个子列表的基本大小
    int groupSize = currentDataGroup.size() / 4;

    // 记录当前切分的起始索引
    int currentIndex = 0;
    // 计算当前子列表的大小
    int size = groupSize > 0 ? groupSize : 1;

    // 循环切分列表
    while (currentIndex + size <= currentDataGroup.size()) {

      // 使用 sublist 方法获取当前子列表
      List<Integer> subList =
          new ArrayList<>(currentDataGroup.subList(currentIndex, currentIndex + size));
      executeDataGroupQueue.add(subList);

      // 更新当前切分的起始索引
      currentIndex += size;
    }
    // 特判最后一组，因为数量可能不齐
    if (currentIndex < currentDataGroup.size()) {
      executeDataGroupQueue.add(
          new ArrayList<>(currentDataGroup.subList(currentIndex, currentDataGroup.size())));
    }

    currentDataGroup.clear();
  }

  @Override
  public Set<String> getDeleteOperationTraces() {
    cascade_operation.clear();
    for (int threadId = 0; threadId < allTxns_.size(); threadId++) {
      for (int trxId = 0; trxId < allTxns_.get(threadId).size(); trxId++) {
        // 如果事务包含当前遍历到的组，那这个事务所有操作不需要执行
        Set<Integer> txnSet = new HashSet<>(txn2Group.get(threadId).get(trxId));
        // 和当前删除的data group取交集
        txnSet.retainAll(currentDataGroup);
        // 交集为空说明没有被当前mutation删除
        boolean dataGroupExist = txnSet.isEmpty();
        if (!dataGroupExist) {
          for (OperationTrace op : allTxns_.get(threadId).get(trxId)) {
            cascade_operation.add(op.getOperationID());
          }
        }
      }
    }

    return cascade_operation;
  }
}
