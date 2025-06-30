package ana.window;

import java.util.HashMap;

/**
 * 针对每一个Thread，维护被加入到analysis window的最大时间戳，即最大结束时间戳
 *
 * @author like_
 */
public class TimestampLine {
  /** 核心数据结构 key:每一个线程的编号 value:对应线程被加入到analysis window的最大时间戳 */
  private static HashMap<String, Long> timestampLineList;

  public static void initialize() {
    timestampLineList = new HashMap<>();
  }

  /**
   * 更新timestampLineList的接口
   *
   * @param threadID
   * @param timestampLine
   */
  public static void updateTimestampLine(String threadID, long timestampLine) {
    timestampLineList.put(threadID, timestampLine);
  }

  /** 获取当前timestampLineList中最小的timestampLine返回 */
  public static long getMinTimestampLine() {
    long minTimestampLine = Long.MAX_VALUE;
    for (String key : timestampLineList.keySet()) {
      if (minTimestampLine > timestampLineList.get(key)) {
        minTimestampLine = timestampLineList.get(key);
      }
    }
    return minTimestampLine;
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return
   */
  public static Object[] getAllObject() {
    return new Object[] {timestampLineList};
  }
}
