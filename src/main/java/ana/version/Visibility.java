package ana.version;

import ana.window.profile.Profile;
import ana.window.profile.ProfileMap;
import trace.OperationTraceType;

/** 常量，用于标识未提交版本对于读一致性时间区间的可见性 */
public enum Visibility {
  VISIBLE,
  INVISIBLE,
  UNCERTAIN;

  /**
   * 假设version
   * chain中仅有uncommittedVersion，判断一个uncommittedVersion是否对于consistentReadTimestampInterval可见
   *
   * @param uncommittedVersion 一个未提交版本
   * @param consistentReadTimestampInterval UNCOMMITTED_READ读操作的读一致性时间区间
   * @return
   */
  public static Visibility isVisible(
      Version uncommittedVersion, Version consistentReadTimestampInterval) {
    if (uncommittedVersion == null || consistentReadTimestampInterval == null) {
      throw new RuntimeException("must not be null");
    }

    // 1.如果uncommittedVersion完全在consistentReadTimestampInterval之后，那么一定看不到
    if (consistentReadTimestampInterval.getFinishTimestamp()
        <= uncommittedVersion.getStartTimestamp()) {
      return Visibility.INVISIBLE;
    }

    // 2.如果uncommittedVersion与consistentReadTimestampInterval重叠，那么不能明确是否能看到
    if (Version.isOverlapping(uncommittedVersion, consistentReadTimestampInterval)) {
      return Visibility.UNCERTAIN;
    }

    // 3.如果uncommittedVersion完全在consistentReadTimestampInterval之前，
    // 那么需要获取uncommittedVersion对应事务的结束时间区间才能进一步判断
    Profile profile = ProfileMap.getProfile(uncommittedVersion.getTransactionID());
    if (profile == null) {
      throw new RuntimeException("must not be null");
    }
    // 创建事务结束操作时间区间的伪version
    Version finalPseudoVersion =
        new Version(profile.getEndStartTimestamp(), profile.getEndFinishTimestamp());
    // 3.1如果finalPseudoVersion完全在consistentReadTimestampInterval之后，那么一定可以看到
    if (consistentReadTimestampInterval.getFinishTimestamp()
        <= finalPseudoVersion.getStartTimestamp()) {
      return Visibility.VISIBLE;
    }

    // 3.2如果finalPseudoVersion与consistentReadTimestampInterval重叠，那么需要根据事务的结束操作类型，进一步判断
    if (Version.isOverlapping(finalPseudoVersion, consistentReadTimestampInterval)) {
      // 3.2.1如果事务的结束操作是提交，那么可以一定可以看到
      if (profile.getEndType() == OperationTraceType.COMMIT) {
        return Visibility.VISIBLE;
      }
      // 3.2.2如果事务的结束操作是回滚，那么不能明确是否能看到
      if (profile.getEndType() == OperationTraceType.ROLLBACK) {
        return Visibility.UNCERTAIN;
      }
    }

    // 3.3如果finalPseudoVersion完全在consistentReadTimestampInterval之前，那么需要根据事务的结束操作类型，进一步判断
    // 3.3.1如果事务的结束操作是提交，那么可以一定可以看到
    if (profile.getEndType() == OperationTraceType.COMMIT) {
      return Visibility.VISIBLE;
    }
    // 3.3.2如果事务的结束操作是回滚，那么一定看不到
    if (profile.getEndType() == OperationTraceType.ROLLBACK) {
      return Visibility.INVISIBLE;
    }

    // 所有的情况上面都考虑了
    throw new RuntimeException("can not reach this");
  }
}
