package ana.verify.adapter;

import ana.version.Version;
import ana.window.profile.Profile;
import trace.IsolationLevel;
import trace.OperationTrace;

/**
 * 结合具体的数据库事务模型完成分析工作
 *
 * @author like_
 */
public abstract class Adapter {
  /**
   * 结合数据库的事务模型，获取当前trace的一致性读时间区间(PS:区别于事务的读一致性时间区间)
   *
   * @param operationTrace 当前trace，即analysis window的第一条trace
   * @param profile 当前trace所在的事务，但是在获取最早读一致性时间区间时，profile与trace不配套,但不影响最终结果
   * @return 返回一个封装一致性读时间区间的伪Version
   */
  public abstract Version getConsistentReadTimeInterval(
      OperationTrace operationTrace, Profile profile);

  /**
   * 结合数据库的事务模型，判断某个隔离级别是否做first-updater-wins的检查
   *
   * @param isolationLevel 隔离级别，一般在RR或以上会有fuw检查
   * @return 如果会做检查，返回true
   */
  public abstract boolean doFirstUpdaterWins(IsolationLevel isolationLevel);

  /**
   * 结合数据库的事务模型，定制事务的读一致性时间区间放入profile中
   *
   * @param profile 事务的profile
   * @param operationTrace 事务的每一个operationTrace
   */
  public abstract void setConsistentReadTimeInterval(
      Profile profile, OperationTrace operationTrace);
}
