package replay.controller.cascadeDetect;

import adapter.Adapter;
import gen.operation.enums.OperationType;
import java.util.*;
import java.util.stream.Collectors;
import replay.controller.ReplayController;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.OperationTraceType;
import trace.TraceUtil;
import util.jdbc.DataSourceUtils;

public class VersionOperationGraph {
  // 两种边
  // 1. chain edge 按数据划分的版本链,每个key value对表示一个数据的版本链,key对应tuple trace里的get key方法，是表名+主键
  private final Map<String, List<OperationTrace>> chainEdge;

  // 2. depend edge用map<string,set<string>>存储 key是被依赖的前面的操作，value是依赖它的操作
  private final Map<String, Set<String>> dependEdge;

  // 由于删除操作所引入的新的依赖关系，如果操作在测试后可以删除，则把这部分关系更新进dependEdge里
  private Map<String, Set<String>> tempDependEdge = new HashMap<>();

  // 从op id到具体内容的映射
  private Map<String, OperationTrace> operationMap;

  private final Map<String, VersionDependencyType> versionDependencyTypeMap;

  public VersionOperationGraph(List<List<List<OperationTrace>>> allTxns) {
    buildOpMap(allTxns);
    chainEdge = new DataChainBuilder().build(operationMap);

    dependEdge = new DependBuilder().build(chainEdge);

    versionDependencyTypeMap = new HashMap<>();
    // 构造初始时的依赖关系
    for (String keyId : dependEdge.keySet()) {
      OperationTrace keyOp = operationMap.get(keyId);
      // 依赖关系是基于被依赖操作的写集生成的
      if (keyOp.getWriteTupleList() == null || keyOp.getWriteTupleList().isEmpty()) {
        continue;
      }

      String dataKeyId = keyOp.getWriteTupleList().get(0).getPrimaryKey();
      for (String dependId : dependEdge.get(keyId)) {
        OperationTrace dependOp = operationMap.get(dependId);
        versionDependencyTypeMap.put(dependId, getDepend(keyOp, dependOp, dataKeyId));
      }
    }
  }

  private void buildOpMap(List<List<List<OperationTrace>>> allTxns) {
    // 将三层list展开，映射到map中
    operationMap =
        allTxns.stream() // Flatten the outer list
            .flatMap(List::stream) // Flatten the middle list
            .flatMap(List::stream) // Flatten the inner list
            .collect(
                Collectors.toMap(OperationTrace::getOperationID, operationTrace -> operationTrace));
  }

  /**
   * 获取操作对应获取快照点的操作id
   *
   * @param operationId 被查询的操作
   * @return 获取快照的操作id
   */
  private String findSnapshotOp(String operationId) {

    Map.Entry<Integer, Integer> ids = ReplayController.op2ids.get(operationId);
    int threadId = ids.getKey();
    int txnId = ids.getValue();
    String[] tmp = operationId.split(",");
    int nowOpId = Integer.parseInt(tmp[tmp.length - 1]);
    OperationType nowOpType =
        TraceUtil.operationTraceType2OperationType(
            operationMap.get(operationId).getOperationTraceType());

    // 目前op id 的 0-0-threadId,txnId,opId, 而start一定是事务的第一个操作，所以op id=0
    int opId = 0;
    String opHeader = String.format("0-0-%d,%d,", threadId, txnId);
    String startOpId = opHeader + opId;
    OperationTrace startOp = operationMap.get(startOpId);

    IsolationLevel isolationLevel = startOp.getIsolationLevel();
    Adapter adapter = DataSourceUtils.getAdapter();

    if (adapter.isCurrentRead(isolationLevel, nowOpType)) {
      return operationId;
    }
    // 记录是不是第一个读或写
    boolean isFirstRead = true, isFirstWrite = true;
    // 按顺序扫描这个事务，找到获取快照的操作
    for (; opId <= nowOpId; opId++) {
      // 提取当前操作的信息
      String snapshotOpId = opHeader + opId;
      // 如果当前操作不需要执行，那直接跳过
      //            if (!ReplayController.isExecute(snapshotOpId)){
      //                continue;
      //            }
      OperationTrace snapshotOp = operationMap.get(snapshotOpId);
      OperationType opType =
          TraceUtil.operationTraceType2OperationType(snapshotOp.getOperationTraceType());
      assert opType != null;

      // 判断是不是获取快照的操作
      if (adapter.isSnapshotPoint(
          isolationLevel,
          opType,
          isFirstRead & TraceUtil.isReadOperation(snapshotOp),
          isFirstWrite & TraceUtil.isWriteOperation(snapshotOp))) {
        return snapshotOpId;
      }
      // 如果是读或者写操作，则后续的操作必不是第一个，把标志位去掉
      if (TraceUtil.isReadOperation(snapshotOp)) {
        isFirstRead = false;
      }
      if (TraceUtil.isWriteOperation(snapshotOp)) {
        isFirstWrite = false;
      }
    }
    // 理论上上面的遍历一定能找到，所以不会走到这里
    return null;
  }

  /**
   * 删除操作后的更新，返回删除当前操作后需要级联删除的操作
   *
   * @param operationId 当前操作
   * @param ignoreOpIds 需要忽略的依赖操作，即将要删除的操作
   * @return 需要级联删除的操作
   */
  public Set<String> updateDepend(String operationId, Set<String> ignoreOpIds) {
    Set<String> deleteOpIds = new HashSet<>();
    Queue<String> cascadeDeleteOpIds = new LinkedList<>();
    tempDependEdge = new HashMap<>();

    cascadeDeleteOpIds.add(operationId);

    Set<String> ignoreOpIdsCopy = new HashSet<>(ignoreOpIds);

    // 尾递归转循环，bfs，保证靠前的操作先被验证（和删除），这样后面的操作在验证时能跳过所有会被删除的操作
    while (!cascadeDeleteOpIds.isEmpty()) {
      String opId = cascadeDeleteOpIds.poll();

      if (deleteOpIds.contains(opId)) {
        continue;
      }
      deleteOpIds.add(opId);
      // 可能没有继续级联依赖的操作了
      if (!dependEdge.containsKey(opId)) {
        continue;
      }

      for (String referOp : dependEdge.get(opId)) {
        // 更新失败，说明这个操作需要删除，在找依赖时要忽略这部分还没被删除的操作
        if (!findDependVersion(referOp, ignoreOpIdsCopy)) {
          cascadeDeleteOpIds.add(referOp);
          ignoreOpIdsCopy.add(referOp);
        }
      }
    }
    return deleteOpIds;
  }

  // 把新更新的依赖加入依赖图中
  private void updateDependEdges() {

    for (String keyId : tempDependEdge.keySet()) {
      if (!dependEdge.containsKey(keyId)) {
        dependEdge.put(keyId, new HashSet<>());
      }
      dependEdge.get(keyId).addAll(tempDependEdge.get(keyId));
    }
  }

  /**
   * 确定当前操作对应的依赖版本（操作）
   *
   * @param operationId 需要找依赖的操作
   * @param ignoreOpIds 忽略的依赖操作
   * @return 找到的依赖版本不合法
   */
  private boolean findDependVersion(String operationId, Set<String> ignoreOpIds) {
    OperationTrace op = operationMap.get(operationId);

    // 控制性语句没有依赖,被删除的操作不需要讨论依赖
    OperationType opType = TraceUtil.operationTraceType2OperationType(op.getOperationTraceType());
    if (opType == OperationType.START
        || opType == OperationType.COMMIT
        || opType == OperationType.ROLLBACK
        || !ReplayController.isExecute(operationId)) {
      return true;
    }

    Set<String> dataSet = TraceUtil.getDataSet(op);

    // 获取的快照操作，在这个时间之前的操作才可能是可见的
    String snapshotOpId = findSnapshotOp(operationId);
    OperationTrace snapshotOp = operationMap.get(snapshotOpId);

    // 对每个数据项找依赖版本
    for (String key : dataSet) {
      List<OperationTrace> chain = chainEdge.get(key);
      // 当前操作的位置
      int opIndex = chain.indexOf(op);

      // 存储可能可见的事务候选集
      Set<String> candidateTxns = new HashSet<>();
      // 操作自己的事务肯定是可见的
      candidateTxns.add(op.getTransactionID());

      // 扫描一下
      int tmpIdx;
      for (tmpIdx = opIndex; tmpIdx >= 0; tmpIdx--) {
        OperationTrace tmpOp = chain.get(tmpIdx);
        // 无视读操作和rollback，无视指定无视的操作和不执行的操作
        if (tmpOp.getOperationTraceType() == OperationTraceType.SELECT
            || tmpOp.getOperationTraceType() == OperationTraceType.ROLLBACK
            || ignoreOpIds.contains(tmpOp.getOperationID())
            || !ReplayController.isExecute(tmpOp.getOperationID())) {
          continue;
        }
        // 可能可见的事务id
        if (tmpOp.getOperationTraceType() == OperationTraceType.COMMIT
            && (snapshotOp == null
                || tmpOp.getStartTimestamp() <= snapshotOp.getFinishTimestamp())) {
          candidateTxns.add(tmpOp.getTransactionID());
        }
        // 可见的操作，判断一下依赖是否合法
        if (candidateTxns.contains(tmpOp.getTransactionID())) {
          VersionDependencyType newDepend = getDepend(tmpOp, op, key);
          if (newDepend == null) {
            return false;
          } else {
            break;
          }
        }
      }

      // 如果找不到，把它当作依赖初始数据的操作，跳过后面添加操作的部分
      if (tmpIdx < 0) {
        continue;
      }

      String dependOp = chain.get(tmpIdx).getOperationID();
      // 添加依赖版本
      if (!tempDependEdge.containsKey(dependOp)) {
        tempDependEdge.put(dependOp, new HashSet<>());
      }
      tempDependEdge.get(dependOp).add(operationId);
    }

    return true;
  }

  // 判断依赖关系，依赖类型以被依赖的操作为准；如果没有合法的依赖关系，返回null
  private VersionDependencyType getDepend(
      OperationTrace dependOp, OperationTrace referOp, String key) {
    OperationType dependType =
        TraceUtil.operationTraceType2OperationType(dependOp.getOperationTraceType());
    OperationType referType =
        TraceUtil.operationTraceType2OperationType(referOp.getOperationTraceType());

    assert dependType != null;
    assert referType != null;

    // 被依赖的操作必须是写操作
    switch (dependType) {
      case DELETE:
        // delete后只可能跟读和删除
        switch (referType) {
          case SELECT:
            // 读必不能读到
            if (referOp.getReadTupleList() != null
                && referOp.getReadTupleList().stream().anyMatch(t -> t.getKey().equals(key))) {
              return null;
            }
            return VersionDependencyType.SELECT;
          case INSERT:
            return VersionDependencyType.INSERT;
          default:
            return null;
        }
      case INSERT:
      case UPDATE:
        // 这两个都是新增一个版本
        switch (referType) {
          case SELECT:
            // 如果读的内容不对，不行
            if (!TraceUtil.isSameVersion(dependOp, referOp, key)) {
              return null;
            }
            return VersionDependencyType.SELECT;
            // 更新的话，区分是不是更新了一个新版本
          case UPDATE:
            if (TraceUtil.isSameVersion(dependOp, referOp, key)) {
              return VersionDependencyType.SKIP_UPDATE;
            }
            return VersionDependencyType.UPDATE;
          case DELETE:
            return VersionDependencyType.DELETE;
          case INSERT: // 不能重复插入
          default:
            return null;
        }
      default:
        return null;
    }
  }

  public void persist() {
    updateDependEdges();
  }
}
