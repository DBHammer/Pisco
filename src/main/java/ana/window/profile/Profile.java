package ana.window.profile;

import ana.version.Version;
import lombok.Getter;
import lombok.Setter;
import trace.IsolationLevel;
import trace.OperationTraceType;

/**
 * 封装一个事务所需要的信息
 *
 * @author like_
 */
@Getter
public class Profile {

  String transactionID;

  /** 事务的开始时间区间 */
  private final long beginStartTimestamp;

  private final long beginFinishTimestamp;

  /** 事务的结束时间区间 */
  @Setter private long endStartTimestamp;

  @Setter private long endFinishTimestamp;

  /** commit rollback */
  @Setter private OperationTraceType endType;

  /** 事务当前的隔离级别 */
  private final IsolationLevel isolationLevel;

  /** 事务是否只读 */
  @Setter private boolean readOnly;

  /** 事务开始时获取一致性快照 */
  private final boolean startWithSnapshot;

  /** 当前事务获取一致性快照的时间区间，即事务获取一致性快照所在操作的开始和结束时间区间 */
  @Setter private Version consistentReadTimeInterval;

  public Profile(
      String transactionID,
      long beginStartTimestamp,
      long beginFinishTimestamp,
      boolean readOnly,
      boolean startWithSnapshot,
      IsolationLevel isolationLevel) {
    super();
    this.transactionID = transactionID;

    this.beginStartTimestamp = beginStartTimestamp;
    this.beginFinishTimestamp = beginFinishTimestamp;

    this.isolationLevel = isolationLevel;

    this.readOnly = readOnly;
    this.startWithSnapshot = startWithSnapshot;

    this.endType = OperationTraceType.START; // 表示事务还没有结束
  }
}
