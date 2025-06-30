package ana.window;

import ana.version.Version;
import java.util.HashMap;
import java.util.Map;

/** 保存当前活跃事务开始操作的时间区间，以Analysis window中第一条trace为基准 */
public class Active {
  /** 核心数据结构，保存当前活跃的事务开始操作的时间区间，key：trasactionID value:开始操作的时间区间 */
  private static Map<String, Version> active;

  public static void initialize() {
    active = new HashMap<>();
  }

  public static void putTransaction(
      String transactionID, long beginStartTimestamp, long beignFinishTimestamp) {
    active.put(transactionID, new Version(beginStartTimestamp, beignFinishTimestamp));
  }

  public static void removeTransaction(String transactionID) {
    if (active.remove(transactionID) == null) {
      throw new RuntimeException("most not be null");
    }
  }

  /**
   * 判断一个事务是否是活跃事务 Note:事务信息加入Active一定要先于加入Dependency Graph，否则DependencyGraph的维护会存在问题
   *
   * @param transactionID 事务id
   */
  public static boolean isActive(String transactionID) {
    return active.get(transactionID) != null;
  }

  /**
   * 找到开始操作时间区间最小的version 这里采用了一种保守的策略，事务的开始时间区间一定完全在事务看到数据的最早时间区间之前或者相等
   * 这里使用事务的开始时间区间而不是事务读一致性时间区间，原因在于：有可能一个事务在获取读一致性时间区间之前，存在不获取读一致性时间区间的读操作
   * 优点：简单有效，缺点：有些版本可能得不到及时的清理，但是最终会被清理且这样的情况是小概率和小数目的事件
   *
   * @return active中开始时间最早的版本
   */
  public static Version getEarliestConsistentReadTimeInterval() {
    Version version;
    Version earliestConsistentReadTimeInterval = null;
    for (String transactionID : active.keySet()) {
      version = active.get(transactionID);
      if (earliestConsistentReadTimeInterval == null
          || version.getStartTimestamp() < earliestConsistentReadTimeInterval.getStartTimestamp()) {
        earliestConsistentReadTimeInterval = version;
      }
    }

    // 如果调度线程触发了Version Chain的清理工作，那么一定会有活跃事务的存在，那么getEarliestConsistentReadTimeInterval一定不会返回null
    // 调度线程，触发Version Chain的清理的三种情况
    // 第一，往Version Chain中添加未提交版本
    // 第二，往Version Chain中添加已提交版本
    // 第三，往Version Chain中添加回滚版本
    // 三种情况，当前版本所在trace仍然在analysis window中，那么一定会有active的事务存在

    // 调度线程，触发Dependency Graph的清理工作，即调度线程向分析窗口中添加新的事务时，我们保证：
    // active 先于依赖图进行维护，那么可以保证一定会有active的事务存在
    if (earliestConsistentReadTimeInterval == null) {
      throw new RuntimeException("must not null");
    }
    return earliestConsistentReadTimeInterval;
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return 所有对象构成的数组
   */
  public static Object[] getAllObject() {
    return new Object[] {active};
  }
}
