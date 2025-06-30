package ana.verify.adapter;

import ana.version.Version;
import ana.window.profile.Profile;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.ReadMode;

public class SQLiteAdapter extends Adapter {
  /**
   * 结合数据库的事务模型，获取当前trace的一致性读时间区间(PS:区别于事务的读一致性时间区间)
   *
   * @param operationTrace 当前trace，即analysis window的第一条trace
   * @param profile 当前trace所在的事务，但是在获取最早读一致性时间区间时，profile与trace不配套,但不影响最终结果
   * @return 返回一个封装一致性读时间区间的伪Version
   */
  public Version getConsistentReadTimeInterval(OperationTrace operationTrace, Profile profile) {
    // 1.SQLite只有锁定读，那么读一致性时间区间为trace的时间区间
    if (operationTrace.getReadMode() == ReadMode.LOCKING_READ) {
      return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
    } else if (operationTrace.getReadMode() == ReadMode.UNCOMMITTED_READ) {
      // 1.2如果是当前读，那么读一致性时间区间为trace的时间区间
      return new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp());
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
    // SQLite没有事务级别的一致性读，类似与MySQL的SR隔离级别
  }
}
