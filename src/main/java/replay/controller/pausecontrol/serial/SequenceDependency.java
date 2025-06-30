package replay.controller.pausecontrol.serial;

import context.OrcaContext;
import java.util.*;
import java.util.stream.Collectors;
import trace.OperationTrace;
import trace.OperationTraceType;

public class SequenceDependency {
  // store operation trace （还未被确定顺序的）操作
  private List<OperationTrace> operationTraceList;

  private List<OperationTrace> operationTraceListRaw;
  // store operation trace dependencies 操作间依赖的邻接表，只维护还未确定执行顺序的部分
  private Map<String, List<String>> operationDependencies;

  private Map<String, List<String>> operationDependenciesRaw;

  public void init() {
    operationTraceList = new ArrayList<>();
    operationDependencies = new HashMap<>();
  }

  public void pinData() {
    // 深拷贝 operationTraceList 到 operationTraceListRaw
    operationTraceListRaw =
        operationTraceList.stream()
            .map(OperationTrace::new) // 假设有一个复制构造函数
            .collect(Collectors.toList());

    // 深拷贝 operationDependencies 到 operationDependenciesRaw
    operationDependenciesRaw =
        operationDependencies.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> new ArrayList<>(entry.getValue())));
  }

  /**
   * add operation trace to operationTraceList it seems that when added, the operation trace is
   * sorted by start timestamp
   *
   * @param trace trace added to operation trace list
   */
  public void addTrace(OperationTrace trace) {
    operationTraceList.add(trace);
  }

  /** 以朴素方法生成操作序列 先n2确定所有偏序关系对，再做拓扑排序 */
  public List<List<String>> generateOperationSequenceNaive() {
    ArrayList<List<String>> sequence = new ArrayList<>();

    // 偏序对
    Map<String, Set<String>> partialOrders = new HashMap<>();

    // 确定所有偏序关系对
    for (int i = 0; i < operationTraceList.size(); i++) {
      OperationTrace op1 = operationTraceList.get(i);
      if (!partialOrders.containsKey(op1.getOperationID())) {
        partialOrders.put(op1.getOperationID(), new HashSet<>());
      }
      for (int j = i + 1; j < operationTraceList.size(); j++) {

        OperationTrace op2 = operationTraceList.get(j);

        String priorID = null;
        String laterID = null;
        // 如果op2早于op1, 放入op1->op2
        if (op1.getStartTimestamp() > op2.getFinishTimestamp()) {
          priorID = op2.getOperationID();
          laterID = op1.getOperationID();
        } // 如果op1早于op2, 放入op2->op1
        else if (op2.getStartTimestamp() > op1.getFinishTimestamp()) {
          priorID = op1.getOperationID();
          laterID = op2.getOperationID();
        }

        if (priorID != null && laterID != null) {
          if (partialOrders.containsKey(priorID)) {
            partialOrders.get(priorID).add(laterID);
          } else {
            Set<String> set = new HashSet<>();
            set.add(laterID);
            partialOrders.put(priorID, set);
          }
        }
      }
    }
    // 还要再加上锁的偏序关系
    for (String opKey : partialOrders.keySet()) {
      if (operationDependencies.containsKey(opKey)) {
        for (String depKey : operationDependencies.get(opKey)) {
          if (partialOrders.containsKey(depKey)) {
            partialOrders.get(opKey).add(depKey);
          }
        }
      }
    }

    // 记录每个项前驱依赖的数量
    Map<String, Integer> frontCnt = new HashMap<>();
    for (Set<String> opKeys : partialOrders.values()) {
      for (String op : opKeys) {
        frontCnt.put(op, frontCnt.getOrDefault(op, 0) + 1);
      }
    }

    List<String> currentKey = new ArrayList<>();
    for (String opKey : partialOrders.keySet()) {
      if (!frontCnt.containsKey(opKey) || frontCnt.get(opKey) == 0) {
        currentKey.add(opKey);
      }
    }
    int cnt = currentKey.size();
    // 拓扑排序
    while (cnt < partialOrders.size()) {
      List<String> nextKey = new ArrayList<>();
      for (String op : currentKey) {
        for (String dep : partialOrders.get(op)) {
          frontCnt.put(dep, frontCnt.get(dep) - 1);
          if (frontCnt.get(dep) == 0) {
            nextKey.add(dep);
          }
        }
      }
      sequence.add(currentKey);
      currentKey = nextKey;
      cnt += currentKey.size();
    }

    return sequence;
  }

  /**
   * 生成操作序列，即对应现在seq.json的内容 下面提供了一些算法抽象出来的方法
   *
   * @return 返回操作序列
   */
  public List<List<String>> generateOperationSequence() {
    ArrayList<List<String>> sequence = new ArrayList<>();
    updateDependencies();
    operationTraceList.sort(Comparator.comparing(OperationTrace::getFinishTimestamp));
    while (!operationTraceList.isEmpty()) {
      Set<String> ret = null;
      Set<OperationTrace> candidates = new HashSet<>();
      for (int i = 0; i < OrcaContext.configColl.getReplay().getSequenceBatchNumber(); i++) {
        candidates.addAll(getCandidateOperationId(operationTraceList.get(0)));
        ret = getNotDependOperationId(candidates);
        if (ret.isEmpty()) { // 如果没有满足条件的op，说明这里有冲突，干脆全加
          ret = candidates.stream().map(OperationTrace::getOperationID).collect(Collectors.toSet());
        }
      }
      //                  System.out.println("ret.size() = " + Objects.requireNonNull(ret).size());

      removeDependency(ret);
      // seq append these key

      List<String> key_list = new ArrayList<>(ret);
      sequence.add(key_list);

      removeOperationTraces(ret);
    }

    return sequence;
  }

  /** 依赖的前驱必须是对应操作所在事务的结束操作，否则会因为2PL的原因卡死 */
  private void updateDependencies() {
    HashMap<String, String> trx2end = new HashMap<>();
    for (OperationTrace trace : operationTraceList) {
      if (trace.getOperationTraceType() == OperationTraceType.COMMIT
          || trace.getOperationTraceType() == OperationTraceType.ROLLBACK) {
        trx2end.put(trace.getTransactionID(), trace.getOperationID());
      }
    }

    for (List<String> frontOp : operationDependencies.values()) {
      frontOp.replaceAll(s -> trx2end.get(s.substring(0, s.lastIndexOf(","))));
    }
  }

  /**
   * add operation dependency to operationDependencies 根据依赖的出边构造邻接表 called by
   * ana/thread/AnalysisThread.java, after "OutputStructure.outputStructure(\"ME:\""
   *
   * @param frontOperationTraceId front operation id
   * @param backOperationTraceId back operation id
   */
  public void addDependency(String frontOperationTraceId, String backOperationTraceId) {
    // add back operation trace's operation id as map's key, front operation trace's operation id as
    // map's value
    // 记录结点的入度
    if (operationDependencies.containsKey(backOperationTraceId)) {
      operationDependencies.get(backOperationTraceId).add(frontOperationTraceId);
    } else {
      List<String> list = new ArrayList<>();
      list.add(frontOperationTraceId);
      operationDependencies.put(backOperationTraceId, list);
    }
  }

  /**
   * add candidate operation ids, whose start timestamp is smaller than the smallest end timestamp
   * 因为我们只知道每个操作的执行时间段，所以我们不知道具体数据库怎么调度时间有重叠的操作顺序。 也就是执行时间段重叠，可能在下一个执行的操作都是候选的操作。
   *
   * @return candidate operation ids
   */
  private Set<OperationTrace> getCandidateOperationId(OperationTrace operationTrace) {
    Long finishTimestamp = operationTrace.getFinishTimestamp();
    Set<OperationTrace> operations;
    //        Set<String> threadIds = new HashSet<>();
    //        for (OperationTrace candidate: operationTraceList){
    //            if (!threadIds.contains(candidate.getThreadID()) && candidate.getStartTimestamp()
    // < finishTimestamp ){
    //                operations.add(candidate);
    //                threadIds.add(candidate.getThreadID());
    //            }
    //
    //            if (threadIds.size() >= OrcaContext.configColl.getLoader().getNumberOfLoader()){
    //                break;
    //            }
    //        }

    operations =
        operationTraceList.stream()
            //            .parallel()
            .filter(op -> op.getStartTimestamp() < finishTimestamp)
            .collect(Collectors.toCollection(HashSet::new));
    return operations;
  }

  /**
   * 从候选集中筛掉有前驱依赖的操作，这些操作不可能在下一个执行，因为要等他们的前驱操作执行完 具体有没有前驱依赖查询邻接表
   *
   * @param operations candidate operation ids
   * @return operation ids that has not front dependency
   */
  private Set<String> getNotDependOperationId(Set<OperationTrace> operations) {
    HashSet<String> notDependOperationIds = new HashSet<>();

    for (OperationTrace op : operations) {
      if (!operationDependencies.containsKey(op.getOperationID())) {
        notDependOperationIds.add(op.getOperationID());
      }
    }
    return notDependOperationIds;
  }

  /**
   * 把要执行的操作从邻接表里删掉，表示他们已经被执行完了，不会阻碍后续操作的执行
   *
   * @param operationIds operation IDs
   */
  private void removeDependency(Set<String> operationIds) {
    //        Set<String> key_null = new HashSet<>();
    //        for (Map.Entry<String, List<String>> entry : operationDependencies.entrySet()) {
    //            List<String> value_list = entry.getValue();
    //            value_list.removeAll(operationIds);
    //            if (value_list.isEmpty()) {
    //                key_null.add(entry.getKey());
    //            }
    //        }
    // 使用流遍历并过滤符合条件的 entry，即 value_list 不包含 operationIds 中的元素
    Set<String> key_null =
        operationDependencies.entrySet().stream()
            .parallel()
            .filter(
                entry -> {
                  List<String> value_list = entry.getValue();
                  value_list.removeAll(operationIds);
                  return value_list.isEmpty();
                })
            .map(Map.Entry::getKey) // 获取满足条件的 entry 的键集合
            .collect(Collectors.toSet());

    // delete all the keys which value list is empty
    for (String str : key_null) {
      operationDependencies.remove(str);
    }
  }

  /**
   * 把要执行的操作从列表里删掉，表示他们已经被执行完了，不会阻碍后续操作的执行
   *
   * @param operationIds 要删除的op id
   */
  private void removeOperationTraces(Set<String> operationIds) {
    // 可能要用迭代器删除(solve)
    //        operationTraceList.removeIf(operationTrace ->
    // operationIds.contains(operationTrace.getOperationID()));

    //        operationTraceList.removeIf(operationTrace ->
    // operationIds.contains(operationTrace.getOperationID()));

    Iterator<OperationTrace> iter = operationTraceList.iterator();

    while (iter.hasNext()) {
      OperationTrace op = iter.next();
      if (operationIds.contains(op.getOperationID())) {
        iter.remove();
        operationIds.remove(op.getOperationID());
      }
      if (operationIds.isEmpty()) {
        break;
      }
    }

    //        operationTraceList = operationTraceList.stream().parallel()
    //                .filter(operationTrace ->
    // !operationIds.contains(operationTrace.getOperationID()))
    //                .collect(Collectors.toList());
  }

  public boolean setOperationListSize(int opSize) {
    if (opSize > operationTraceListRaw.size()) {
      return false;
    }

    // 深拷贝 operationTraceList 到 operationTraceListRaw
    operationTraceList =
        operationTraceListRaw.stream()
            .map(OperationTrace::new) // 假设有一个复制构造函数
            .collect(Collectors.toList());

    Set<String> opKey = new HashSet<>();
    for (int i = opSize; i < operationTraceList.size(); i++) {
      opKey.add(operationTraceList.get(i).getOperationID());
    }
    operationTraceList = operationTraceList.subList(0, opSize);

    // 深拷贝 operationDependencies 到 operationDependenciesRaw
    operationDependencies =
        operationDependenciesRaw.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> new ArrayList<>(entry.getValue())));
    removeDependency(opKey);
    return true;
  }
}
