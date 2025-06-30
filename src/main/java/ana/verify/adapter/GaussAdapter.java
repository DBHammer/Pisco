package ana.verify.adapter;

import ana.main.Config;
import ana.version.Version;
import ana.window.profile.Profile;
import trace.OperationTrace;
import trace.OperationTraceType;

public class GaussAdapter extends PostgreSQLAdapter {
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

      if (Config.SNAPSHOT_POINT.equals("start")) {
        if (operationTrace.getOperationTraceType() == OperationTraceType.START) {
          profile.setConsistentReadTimeInterval(
              new Version(operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp()));
        }
      } else {
        // 2.PG在事务的第一个非控制语句中获取事务的一致性快照
        // PG不支持start transaction with consistent snapshot
        if (operationTrace.getOperationTraceType() != OperationTraceType.START) {
          if (operationTrace.getOperationTraceType() != OperationTraceType.SET_SNAPSHOT) {
            profile.setConsistentReadTimeInterval(
                new Version(
                    operationTrace.getStartTimestamp(), operationTrace.getFinishTimestamp()));
          } else {
            // 3.如果是PG的SET_SNAPSHOT语句，那么将SET_SNAPSHOT语句带有的一致性时间区间设置为当前事务的读一致性时间区间
            try {
              // 3.1按照指定的trace格式解析SET_SNAPSHOT语句从中获取快照
              String[] snapshot =
                  operationTrace
                      .getWriteTupleList()
                      .get(0)
                      .getValueMap()
                      .get("SNAPSHOT")
                      .split(",");

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
}
