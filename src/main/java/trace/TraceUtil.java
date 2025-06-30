package trace;

import gen.operation.enums.OperationType;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class TraceUtil {
  public static OperationTraceType OperationType2TraceType(OperationType operationType) {
    switch (operationType) {
      case INSERT:
        return OperationTraceType.INSERT;
      case DELETE:
        return OperationTraceType.DELETE;
      case SELECT:
        return OperationTraceType.SELECT;
      case UPDATE:
        return OperationTraceType.UPDATE;
      case START:
        return OperationTraceType.START;
      case COMMIT:
        return OperationTraceType.COMMIT;
      case ROLLBACK:
        return OperationTraceType.ROLLBACK;
      case DDL:
        return OperationTraceType.DDL;
      case DistributeSchedule:
        return OperationTraceType.DistributeSchedule;
      case Fault:
        return OperationTraceType.FAULT;
      default:
        throw new RuntimeException("不支持的类型 " + operationType.name());
    }
  }

  public static OperationType operationTraceType2OperationType(OperationTraceType traceType) {
    switch (traceType) {
      case START:
        return OperationType.START;
      case COMMIT:
        return OperationType.COMMIT;
      case DELETE:
        return OperationType.DELETE;
      case INSERT:
        return OperationType.INSERT;
      case SELECT:
        return OperationType.SELECT;
      case UPDATE:
        return OperationType.UPDATE;
      case ROLLBACK:
        return OperationType.ROLLBACK;
      case DDL:
        return OperationType.DDL;
      case DistributeSchedule:
        return OperationType.DistributeSchedule;
      case FAULT:
        return OperationType.Fault;
      case SET_SNAPSHOT:
      default:
        return null;
    }
  }

  /**
   * separate thread id from operation id, format is x-x-thread_id,x,x
   *
   * @param operationId operation id
   * @return thread id
   */
  public static int getThreadIdFromOpId(String operationId) {
    String[] id = operationId.split(",");
    String[] txIds = id[0].split("-");
    return Integer.parseInt(txIds[2]);
  }

  /**
   * separate txn id from operation id, format is x-x-x,txn_id,x
   *
   * @param operationId operation id
   * @return txn id
   */
  public static int getTxnIdFromOpId(String operationId) {
    String[] id = operationId.split(",");
    return Integer.parseInt(id[1]);
  }

  /**
   * 获取trace的特定数据
   *
   * @param op trace
   * @param key 对应数据的key
   * @return 整个数据
   */
  public static TupleTrace getTupleTrace(OperationTrace op, String key) {
    TupleTrace tuple = null;
    if (op.getReadTupleList() != null) {
      tuple =
          op.getReadTupleList().stream()
              .filter(t -> t.getKey().equals(key))
              .findFirst()
              .orElse(null);
    }
    if (op.getWriteTupleList() != null && tuple == null) {
      tuple =
          op.getWriteTupleList().stream()
              .filter(t -> t.getKey().equals(key))
              .findFirst()
              .orElse(null);
    }
    return tuple;
  }

  /**
   * 检查数据是否一致
   *
   * @param dependVersion 前一个tuple
   * @param referVersion 后一个tuple
   * @return 一致
   */
  public static boolean isSameTuple(TupleTrace dependVersion, TupleTrace referVersion) {
    // 安全性检查：是同个主键
    if (!dependVersion.getKey().equals(referVersion.getKey())) {
      return false;
    }
    // 检查每个数据项
    return dependVersion.getValueMap().entrySet().stream()
        .allMatch(entry -> entry.getValue().equals(referVersion.getValueMap().get(entry.getKey())));
  }

  /**
   * 返回操作访问的数据键集合
   *
   * @param op 指定操作
   * @return 操作返回的数据，表示为表名+主键值的集合
   */
  public static Set<String> getDataSet(OperationTrace op) {
    Set<String> dataSet = new HashSet<>();
    if (op.getWriteTupleList() != null) {
      dataSet.addAll(
          op.getWriteTupleList().stream().map(TupleTrace::getKey).collect(Collectors.toSet()));
    }
    if (op.getReadTupleList() != null) {
      dataSet.addAll(
          op.getReadTupleList().stream().map(TupleTrace::getKey).collect(Collectors.toSet()));
    }
    return dataSet;
  }

  /**
   * 判断两个操作在指定key上是不是相同版本
   *
   * @param dependOp 前一个操作
   * @param referOp 后一个操作
   * @param key 数据的键：表名+主键数值
   * @return 版本相同
   */
  public static boolean isSameVersion(OperationTrace dependOp, OperationTrace referOp, String key) {
    // 获取这个操作的数据
    TupleTrace dependVersion = TraceUtil.getTupleTrace(dependOp, key);

    if (dependVersion == null) return false;
    TupleTrace referVersion = TraceUtil.getTupleTrace(referOp, key);

    return TraceUtil.isSameTuple(dependVersion, referVersion);
  }

  /**
   * 判断是不是写操作
   *
   * @param operation 需要判断的操作
   * @return 是写操作
   */
  public static boolean isWriteOperation(OperationTrace operation) {
    OperationType opType =
        TraceUtil.operationTraceType2OperationType(operation.getOperationTraceType());

    return opType == OperationType.INSERT
        || opType == OperationType.DELETE
        || opType == OperationType.UPDATE;
  }
  /**
   * 判断是不是读操作
   *
   * @param operation 需要判断的操作
   * @return 是写操作
   */
  public static boolean isReadOperation(OperationTrace operation) {
    OperationType opType =
        TraceUtil.operationTraceType2OperationType(operation.getOperationTraceType());

    return opType == OperationType.SELECT;
  }
}
