package ana.verify.adapter;

import trace.IsolationLevel;

/** CRDB只有一个串行化隔离级别，通过MVTO实现，即使得依赖图仅有从老事务指向新事务的依赖边，从而消除环路 */
public class CRDBAdapter extends PostgreSQLAdapter {

  /**
   * 判断数据库的某个隔离级别是否做first-updater-wins的检查
   *
   * @param isolationLevel 隔离级别
   * @return crdb不检查FUW，恒为false
   */
  @Override
  public boolean doFirstUpdaterWins(IsolationLevel isolationLevel) {
    return false;
  }
}
