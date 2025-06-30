package ana.verify.adapter;

import ana.version.Version;
import ana.window.profile.Profile;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.ReadMode;

public class MySQLAdapter extends Adapter {
  /**
   * 结合数据库的事务模型，获取当前trace的一致性读时间区间(PS:区别于事务的读一致性时间区间)
   *
   * @param operationTrace 当前trace，即analysis window的第一条trace
   * @param profile 当前trace所在的事务，但是在获取最早读一致性时间区间时，profile与trace不配套,但不影响最终结果
   * @return 返回一个封装一致性读时间区间的伪Version
   */
  public Version getConsistentReadTimeInterval(OperationTrace operationTrace, Profile profile) {
    // 1.根据profile和trace当前的信息，确定ConsistentReadTimeInterval
    if (operationTrace.getReadMode() == ReadMode.LOCKING_READ) {
      // 1.1如果是锁定读，那么读一致性时间区间为trace的时间区间
      return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
    } else if (operationTrace.getReadMode() == ReadMode.UNCOMMITTED_READ) {
      // 1.2如果是当前读，那么读一致性时间区间为trace的时间区间
      return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
    } else if (operationTrace.getReadMode() == ReadMode.CONSISTENT_READ) {
      // 1.3如果是快照读，那么读一致性时间区间取决于隔离级别
      if (profile.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
        // 1.3.1读一致性时间区间为trace的时间区间
        return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
      } else if (profile.getIsolationLevel() == IsolationLevel.REPEATABLE_READ) {
        // 1.3.2获取事务中第一个获取一致性快照的操作的时间区间
        Version consistentReadTimeInterval = profile.getConsistentReadTimeInterval();
        if (consistentReadTimeInterval == null) {
          // 如果trace是一个一致性读的操作，那么对应事务的一致性读区间一定不为null
          throw new RuntimeException("transaction consistentReadTimeInterval must not be null");
        }
        // 关于start with snapshot的一致性时间区间获取，start with
        // snapshot改变了RR隔离级别下获取一致性快照的时间，仅此而已，对其他隔离级别没有任何影响
        // 参见MySQL官方文档 https://dev.mysql.com/doc/refman/8.0/en/commit.html
        return consistentReadTimeInterval;
      } else if (profile.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
        // 1.3.3获取事务中第一个获取一致性快照的操作的时间区间
        Version consistentReadTimeInterval = profile.getConsistentReadTimeInterval();
        if (consistentReadTimeInterval == null) {
          // 如果trace是一个一致性读的操作，那么对应事务的一致性读区间一定不为null
          throw new RuntimeException("transaction consistentReadTimeInterval must not be null");
        }
        return consistentReadTimeInterval;
      }
    }
    throw new RuntimeException("consistentReadTimeInterval must not be null");
  }

  /**
   * 判断数据库的某个隔离级别是否做first-updater-wins的检查
   *
   * @param isolationLevel
   * @return
   */
  @Override
  public boolean doFirstUpdaterWins(IsolationLevel isolationLevel) {
    return false;
  }

  /**
   * 结合数据库的事务模型，定制事务的读一致性时间区间放入profile中
   *
   * @param profile 事务的profile
   * @param operationTrace 事务的每一个operationTrace
   */
  @Override
  public void setConsistentReadTimeInterval(Profile profile, OperationTrace operationTrace) {
    // 找到事务中第一个采用一致性读的操作作为事务的读一致性时间区间
    if (profile.getConsistentReadTimeInterval() == null) {

      // MySQL在第一个一致性读操作获取事务的一致性快照
      // 可能在事务开始时获取一致性快照，支持start transaction with consistent snapshot
      if (operationTrace.getReadMode() == ReadMode.CONSISTENT_READ) {
        profile.setConsistentReadTimeInterval(
            new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp()));
      }
    }
  }
}
