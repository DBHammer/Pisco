package trace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.Data;
import util.time.DatetimeUtil;
import util.time.TimestampBase;

/**
 * 代表一个operation的trace信息
 *
 * @author like_
 */
@Data
public class OperationTrace implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 所属文件 */
  private String traceFile;

  /** s operation所在线程的ID */
  private String threadID;

  /** operation所在事务的ID threadID,transactionID，可以唯一地标识一个事务 */
  private String transactionID;

  /** operation的ID,即operation在事务中的ID threadID,transactionID,operationID，可以唯一地标识一个操作 */
  private String operationID;

  /** debug信息：该操作对应的SQL */
  private String sql;

  /** OperationTrace的类型，共五种 */
  private OperationTraceType operationTraceType;

  /** debug信息：开始时间戳 */
  private Long beforeLoadTimestamp;

  /** 加载之前时间戳 */
  private Long startTimestamp;

  /** 结束时间戳 */
  private Long finishTimestamp;

  /** debug信息：开始时间戳 */
  private Long beforeLoadTimestampMills;

  /** debug信息：加载之前时间戳 */
  private Long startTimestampMills;

  /** debug信息：结束时间戳 */
  private Long finishTimestampMills;

  /** debug信息：开始时间戳 */
  private String beforeLoadTimestampMillsHumanReadable;

  /** debug信息：加载之前时间戳 */
  private String startTimestampMillsHumanReadable;

  /** debug信息：结束时间戳 */
  private String finishTimestampMillsHumanReadable;

  /** Isolation Level */
  private IsolationLevel isolationLevel;

  /** Exception */
  private String exception;

  /** 读写所针对的predicate */
  private String predicateLock;

  /** 操作的加锁模式 */
  private TraceLockMode traceLockMode;

  /** 操作读取模式 */
  private ReadMode readMode;

  /** operation的读单元集合 */
  private List<TupleTrace> readTupleList;

  /** operation的写单元集合 */
  private List<TupleTrace> writeTupleList;

  /** SavePoint */
  private String savepoint;

  /** isValid 保存Where判别结果 */
  private Boolean isWhereValid;

  public OperationTrace(OperationTrace trace) {
    this.setBeforeLoadTimestampMillsHumanReadable(trace.getBeforeLoadTimestampMillsHumanReadable());
    this.setBeforeLoadTimestamp(trace.getBeforeLoadTimestamp());
    this.setBeforeLoadTimestampMills(trace.getBeforeLoadTimestampMills());

    this.setStartTimestamp(trace.getStartTimestamp());
    this.setStartTimestampMills(trace.getStartTimestampMills());
    this.setStartTimestampMillsHumanReadable(trace.getStartTimestampMillsHumanReadable());

    this.setFinishTimestamp(trace.getFinishTimestamp());
    this.setFinishTimestampMills(trace.getFinishTimestampMills());
    this.setFinishTimestampMillsHumanReadable(trace.getFinishTimestampMillsHumanReadable());

    //		setStartTime(trace.getStartTimestamp());
    //		setFinishTime(trace.getFinishTimestamp());

    traceFile = trace.traceFile;
    threadID = trace.getThreadID();
    transactionID = trace.getTransactionID();
    operationID = trace.getOperationID();

    sql = trace.getSql();
    operationTraceType = trace.getOperationTraceType();
    isolationLevel = trace.getIsolationLevel();
    predicateLock = trace.getPredicateLock();
    exception = trace.getException();
    traceLockMode = trace.getTraceLockMode();
    readMode = trace.getReadMode();

    if (trace.getReadTupleList() != null) {
      readTupleList = new ArrayList<>(trace.getReadTupleList());
    }
    if (trace.getWriteTupleList() != null) {
      writeTupleList = new ArrayList<>(trace.getWriteTupleList());
    }

    savepoint = trace.getSavepoint();
    isWhereValid = trace.getIsWhereValid();
  }

  /**
   * 设置 加载前时间戳
   *
   * @param nanoTime nanoTime
   */
  public void setBeforeLoadTime(long nanoTime) {
    long millsTime = TimestampBase.nanoTime2MillsTimestamp(nanoTime);

    this.setBeforeLoadTimestamp(nanoTime);
    this.setBeforeLoadTimestampMills(millsTime);
    this.setBeforeLoadTimestampMillsHumanReadable(DatetimeUtil.df.format(new Date(millsTime)));
  }

  /**
   * 设置 开始时间戳
   *
   * @param nanoTime nanoTime
   */
  public void setStartTime(long nanoTime) {
    long millsTime = TimestampBase.nanoTime2MillsTimestamp(nanoTime);

    this.setStartTimestamp(nanoTime);
    this.setStartTimestampMills(millsTime);
    this.setStartTimestampMillsHumanReadable(DatetimeUtil.df.format(new Date(millsTime)));
  }

  /**
   * 设置 结束时间戳
   *
   * @param nanoTime nanoTime
   */
  public void setFinishTime(long nanoTime) {
    long millsTime = TimestampBase.nanoTime2MillsTimestamp(nanoTime);

    this.setFinishTimestamp(nanoTime);
    this.setFinishTimestampMills(millsTime);
    this.setFinishTimestampMillsHumanReadable(DatetimeUtil.df.format(new Date(millsTime)));
  }

  public OperationTrace(String threadID, String transactionID, String operationID) {
    super();
    this.threadID = threadID;
    this.transactionID = threadID + LINKER + transactionID;
    this.operationID = threadID + LINKER + transactionID + LINKER + operationID;
  }

  public void clearDebugInfo() {
    this.setSql(null);

    this.setBeforeLoadTimestamp(null);
    this.setBeforeLoadTimestampMills(null);
    this.setBeforeLoadTimestampMillsHumanReadable(null);

    this.setStartTimestampMills(null);
    this.setStartTimestampMillsHumanReadable(null);

    this.setFinishTimestampMills(null);
    this.setFinishTimestampMillsHumanReadable(null);

    if (readTupleList != null) {
      readTupleList.forEach(TupleTrace::clearDebugInfo);
    }

    if (writeTupleList != null) {
      writeTupleList.forEach(TupleTrace::clearDebugInfo);
    }
  }

  /**
   * OperationTrace <a href="https://www.cnblogs.com/liuwt365/p/7222549.html">...</a>
   *
   * @return
   */
  public static boolean isOverlapping(
      OperationTrace operationTrace1, OperationTrace operationTrace2) {
    // max(start) > min(finish),则两个区间不相交
    if (Math.max(operationTrace1.getStartTimestamp(), operationTrace2.getStartTimestamp())
        > Math.min(operationTrace1.getFinishTimestamp(), operationTrace2.getFinishTimestamp())) {
      return false;
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof OperationTrace) {
      return this.operationID.equals(((OperationTrace) o).getOperationID());
    }
    return false;
  }

  public static final String LINKER = ",";
}
