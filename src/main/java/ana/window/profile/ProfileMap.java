package ana.window.profile;

import ana.main.Config;
import java.util.HashMap;
import java.util.Set;
import trace.OperationTrace;
import trace.OperationTraceType;

/**
 * 维护所有正在分析事务的相关信息 ProfileMap状态与AnalysisWindow同步， 即Dispatch线程往Analysis
 * Window中添加trace的同时往profileMap中新增信息， 当Analysis Thread分析trace结束时，丢弃Analysis
 * Window中trace的同时丢弃profileMap中的trace对应的信息
 *
 * @author like_
 */
public class ProfileMap {
  /** key:事务的id value:事务的profile */
  private static HashMap<String, Profile> profileMap;

  public static void initialize() {
    profileMap = new HashMap<>();
  }

  /**
   * 当遇到一个事务的开始操作，新增一个profile
   *
   * @param transactionID 事务id
   * @param profile 事务开始的操作所形成的profile
   */
  public static void addProfile(String transactionID, Profile profile) {
    profileMap.put(transactionID, profile);
  }

  /**
   * 当遇到一个事务的写操作时，设置该事务为读写事务
   *
   * @param transactionID 事务id
   */
  public static void setReadWrite(String transactionID) {
    Profile profile = profileMap.get(transactionID);
    if (profile == null) {
      throw new RuntimeException("profile must not be null: " + transactionID);
    }

    if (profile.isReadOnly()) {
      throw new RuntimeException("read-only transaction can not have any write operation");
    }

    profile.setReadOnly(false);
  }

  /**
   * 当遇到一个事务的结束操作时，完善该事务的profile信息
   *
   * @param transactionID 事务id
   * @param endStartTimestamp 结束操作的startTime
   * @param endFinishTimestamp 结束操作的finishTime
   * @param operationTraceType commit/rollback
   */
  public static void perfectProfile(
      String transactionID,
      long endStartTimestamp,
      long endFinishTimestamp,
      OperationTraceType operationTraceType) {
    Profile profile = profileMap.get(transactionID);
    if (profile == null) {
      throw new RuntimeException("profile must not be null: " + transactionID);
    }

    profile.setEndStartTimestamp(endStartTimestamp);
    profile.setEndFinishTimestamp(endFinishTimestamp);
    profile.setEndType(operationTraceType);
  }

  /**
   * 设置trace所在事务的读一致性时间区间
   *
   * @param operationTrace trace
   */
  public static void setConsistentReadTimeInterval(OperationTrace operationTrace) {
    Profile profile = getProfile(operationTrace.getTransactionID());
    if (profile == null) {
      throw new RuntimeException("profile must not be null: " + operationTrace.getTransactionID());
    }

    // 具体设置细节，交由adapter处理
    Config.adapter.setConsistentReadTimeInterval(profile, operationTrace);
  }

  /**
   * 判断事务transactionID的profile是否完善
   *
   * @param transactionID 事务id
   * @return 如果包含结束操作则为true
   */
  public static boolean isPerfect(String transactionID) {
    Profile profile = profileMap.get(transactionID);
    if (profile == null) {
      return false;
    }
    return profile.getEndType() == OperationTraceType.COMMIT
        || profile.getEndType() == OperationTraceType.ROLLBACK
        || profile.getEndType() == OperationTraceType.DDL
        || profile.getEndType() == OperationTraceType.FAULT;
  }

  /**
   * 获取一个指定的profile
   *
   * @param transactionID 事务id
   */
  public static Profile getProfile(String transactionID) {
    return profileMap.get(transactionID);
  }

  /**
   * 清理事务的profile
   *
   * @param transactionID 事务id
   */
  public static void removeProfile(String transactionID) {
    profileMap.remove(transactionID);
  }

  /**
   * 返回当前profileMap中所有的事务ID组成的set
   *
   * @return 事务id
   */
  public static Set<String> getAllProfile() {
    return profileMap.keySet();
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return 对象
   */
  public static Object[] getAllObject() {
    return new Object[] {profileMap};
  }
}
