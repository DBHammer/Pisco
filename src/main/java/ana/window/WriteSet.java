package ana.window;

import ana.version.Version;
import ana.window.profile.ProfileMap;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import trace.OperationTrace;
import trace.OperationTraceType;
import trace.TupleTrace;

/**
 * 封装两个WriteSet
 *
 * @author like_
 */
public class WriteSet {

  /**
   * 服务于读一致性验证的writeSet，方便追踪一个每一个时刻所产生的version 加入时机：从Analysis
   * Window头部移除一个trace时，如果该trace产生Version（只考虑未提交version），则将产生的version加入writeSet 移除时机：从Analysis
   * Window头部移除一个trace时，如果该trace是提交或者回滚操作，则将该事务产生的version移除writeSet key:transactionID事务的唯一标识
   * value:一个写入的所有Version。key：version的唯一标识tableID、PK，value:事务写入的Version
   */
  @Getter private static HashMap<String, HashMap<String, Version>> readConsistency;

  /**
   * 服务于History Version的writeSet，方便找到一个事务产生的所有已提交version
   * 加入时机：当调度线程调度一个trace到上下文时，如果该trace产生Version（包括三类version），则将产生的version加入writeSet 移除时机：取决于History
   * Version的清理算法，即当某个Version不需要保留在History Version中时，可以将其从Write Set中移除 key:transactionID事务的唯一标识
   * value:一个写入的所有Version。key：version的唯一标识，value:事务写入的Version
   */
  @Getter private static HashMap<String, HashMap<String, Version>> historyVersion;

  public static void initialize() {
    readConsistency = new HashMap<>();
    historyVersion = new HashMap<>();
  }

  /**
   * 添加一个单独的version到writeSet中 只用于history version
   *
   * @param writeSet
   * @param transactionID
   * @param key
   * @param newVersion
   */
  public static void addVersion(
      HashMap<String, HashMap<String, Version>> writeSet,
      String transactionID,
      String key,
      Version newVersion) {
    // 1.如果writeSet（history version）不存在transactionVersion，那么创建一个并放入writeSet（history version）
    HashMap<String, Version> transactionVersion =
        writeSet.computeIfAbsent(transactionID, k -> new HashMap<>());

    // 2.对于write set(history version)，如果待添加版本是由删除操作产生的，需要检查是否存在一个被覆盖的版本且该覆盖版本是由于插入操作产生的
    if (newVersion.getProducerType() == OperationTraceType.DELETE) {
      if (transactionVersion.get(key) != null
          && transactionVersion.get(key).getProducerType() == OperationTraceType.INSERT) {
        // 2.1那么从transactionVersion移除该版本
        transactionVersion.remove(key);
        return;
      }
    }

    // 3.添加一个version到writeSet（history version）
    Version oldVersion = transactionVersion.get(key);
    // 3.1如果会覆盖旧版本，且如果旧版本的产生是由于Insert操作产生的，那么新版本需要继承旧版本的产生操作类型
    if (oldVersion != null && oldVersion.getProducerType() == OperationTraceType.INSERT) {
      newVersion.setProducerType(
          OperationTraceType.INSERT); // 注意这里修改了newVersion的信息，由于historyVersion和writeSet（History
      // Version）被加入的是同一个version对象，那么historyVersion的version对象也会被修改，所以导致historyVersion的version对象中获取的产生操作类型会存在错误，但是不会对目前的程序产生影响
    }
    transactionVersion.put(key, newVersion);
  }

  /**
   * 将一条operation trace可能会产生的version加入到write set中 只用于read consistency
   *
   * @param writeSet
   * @param operationTrace
   */
  public static void addTransactionVersions(
      HashMap<String, HashMap<String, Version>> writeSet, OperationTrace operationTrace) {
    if (operationTrace.getOperationTraceType() != OperationTraceType.INSERT
        && operationTrace.getOperationTraceType() != OperationTraceType.DELETE
        && operationTrace.getOperationTraceType() != OperationTraceType.UPDATE) {
      throw new RuntimeException("must be insert,delete,update");
    }
    // 1.如果writeSet（read consistency）不存在transactionVersion，那么创建一个并放入writeSet（read consistency）
    HashMap<String, Version> transactionVersion =
        writeSet.computeIfAbsent(operationTrace.getTransactionID(), k -> new HashMap<>());

    List<TupleTrace> writeTupleList = operationTrace.getWriteTupleList();
    if (writeTupleList != null) {
      for (TupleTrace tupleTrace : writeTupleList) {
        transactionVersion.put(
            tupleTrace.getKey(),
            new Version(
                tupleTrace.getValueMap(),
                operationTrace.getOperationID(),
                operationTrace.getOperationTraceType(),
                operationTrace.getTraceFile(),
                operationTrace.getSql()));
      }
    }
  }

  /**
   * 移除某个事务的某个version，如果某个事务的所有version都被移除，那么清理该事务的profile和writeSet 只用于history version
   *
   * @param writeSet
   * @param transactionID
   * @param key
   */
  public static void removeVersion(
      HashMap<String, HashMap<String, Version>> writeSet, String transactionID, String key) {
    // 1.移除对应的version
    if (writeSet.get(transactionID).remove(key) == null) {
      throw new RuntimeException("must not be null");
    }

    // 2.判断是否可以移除profile
    if (writeSet.get(transactionID).isEmpty()) {
      // 2.1清理profile
      ProfileMap.removeProfile(transactionID);
      // 2.2清理writeSet
      writeSet.remove(transactionID);
    }
  }

  /**
   * 移除一个事务所有的Version,并返回 只用于read consistency
   *
   * @param writeSet
   * @param transactionID
   * @return
   */
  public static HashMap<String, Version> removeTransactionVersions(
      HashMap<String, HashMap<String, Version>> writeSet, String transactionID) {
    return writeSet.remove(transactionID);
  }

  /**
   * 获取一个事务所产生的所有version
   *
   * @param writeSet
   * @param transactionID
   * @return
   */
  public static HashMap<String, Version> getTransactionVersions(
      HashMap<String, HashMap<String, Version>> writeSet, String transactionID) {
    return writeSet.get(transactionID);
  }

  /**
   * 判断一个事务的Write Set中所有的Version是否与其前驱检查过WW依赖 只用于history version
   *
   * @param writeSet
   * @param transactionID
   * @return
   */
  public static boolean allIsWW(
      HashMap<String, HashMap<String, Version>> writeSet, String transactionID) {
    // 1.获取transactionID的writeSet
    HashMap<String, Version> transactionVersions = writeSet.get(transactionID);
    if (transactionVersions == null) {
      return true;
    }

    // 2.检查transactionID的writeSet的所有Version是否都经过了与其前驱的WW依赖检查
    for (String key : transactionVersions.keySet()) {
      if (!transactionVersions.get(key).isWW()) {
        return false;
      }
    }
    return true;
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return
   */
  public static Object[] getAllObject() {
    return new Object[] {readConsistency, historyVersion};
  }
}
