package ana.verify.adapter;

import ana.version.Version;
import ana.window.profile.Profile;
import trace.IsolationLevel;
import trace.OperationTrace;
import trace.OperationTraceType;

public class OceanbaseAdapter extends OracleAdapter {

  /**
   * 判断数据库的某个隔离级别是否做first-updater-wins的检查
   *
   * @param isolationLevel 使用的隔离级别
   * @return 是否做first-updater-wins的检查
   */
  @Override
  public boolean doFirstUpdaterWins(IsolationLevel isolationLevel) {
    // OB隔离级别本质上跟Oracle一样，但是MySQL模式下隔离级别的名字与MySQL兼容
    return isolationLevel != IsolationLevel.SERIALIZABLE
        || isolationLevel != IsolationLevel.REPEATABLE_READ;
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

      // 获取事务中第一个获取一致性快照的操作的时间区间, OCEANBASE是在第一个非事务控制语句获取一致性快照的
      if (operationTrace.getOperationTraceType() != OperationTraceType.START) {
        profile.setConsistentReadTimeInterval(
            new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp()));
      }
    }
  }
}
