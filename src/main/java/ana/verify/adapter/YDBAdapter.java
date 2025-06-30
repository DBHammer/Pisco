package ana.verify.adapter;

import ana.version.Version;
import ana.window.profile.Profile;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.OperationTraceType;
import trace.ReadMode;

/** YDB的事务模型与PG类似 */
public class YDBAdapter extends Adapter {
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
      // 1.1如果是锁定读，那么error，pg没有锁定读
      throw new RuntimeException("pg has not Locking Read");
    } else if (operationTrace.getReadMode() == ReadMode.UNCOMMITTED_READ) {
      // 1.2如果是未提交读，那么error，pg没有锁定读
      throw new RuntimeException("pg has not Uncommitted Read");
    } else if (operationTrace.getReadMode() == ReadMode.CONSISTENT_READ) {
      // 1.3如果是快照读，那么读一致性时间区间取决于隔离级别
      if (profile.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED
          || profile.getIsolationLevel() == IsolationLevel.READ_COMMITTED) {
        // 1.3.1读一致性时间区间为trace的时间区间
        return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
      } else if (profile.getIsolationLevel() == IsolationLevel.REPEATABLE_READ
          || profile.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
        // 1.3.2获取事务中第一个获取一致性快照的操作的时间区间
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
    // PG的RR、SR隔离级别需要检查FirstUpdaterWins
    if (isolationLevel == IsolationLevel.SERIALIZABLE
        || isolationLevel == IsolationLevel.REPEATABLE_READ) {
      return true;
    }
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
    // 1.找到事务中第一个采用一致性读的操作作为事务的读一致性时间区间
    if (profile.getConsistentReadTimeInterval() == null) {

      // 2.PG在事务的第一个非控制语句中获取事务的一致性快照
      // PG不支持start transaction with consistent snapshot
      if (operationTrace.getOperationTraceType() != OperationTraceType.START) {
        if (operationTrace.getOperationTraceType() != OperationTraceType.SET_SNAPSHOT) {
          profile.setConsistentReadTimeInterval(
              new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp()));
        } else {
          // 3.如果是PG的SET_SNAPSHOT语句，那么将SET_SNAPSHOT语句带有的一致性时间区间设置为当前事务的读一致性时间区间
          try {
            // 3.1按照指定的trace格式解析SET_SNAPSHOT语句从中获取快照
            String[] snapshot =
                operationTrace.getWriteTupleList().get(0).getValueMap().get("SNAPSHOT").split(",");

            profile.setConsistentReadTimeInterval(
                new Version(Long.parseLong(snapshot[0]), Long.parseLong(snapshot[1])));
          } catch (Exception e) {
            throw new RuntimeException("there is a error in workload trace about set snapshot");
          }
        }
      }
    }
  }
}
