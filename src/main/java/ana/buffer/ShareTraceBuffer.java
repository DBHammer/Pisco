package ana.buffer;

import ana.main.Config;
import ana.main.OrcaVerify;
import ana.thread.SortThread;
import java.util.LinkedList;
import java.util.ListIterator;
import lombok.Getter;
import lombok.Setter;
import trace.OperationTrace;

/**
 * 所有Thread共享的Trace Buffer
 *
 * @author like_
 */
public class ShareTraceBuffer {
  /** 核心数据结构：缓存所有trace file中的trace，按照OperationTrace的开始时间逆序排列 */
  private final LinkedList<OperationTrace> shareBuffer;

  /**
   * 警戒线等于所有Private Trace Buffer中开始时间最小的Trace的开始时间 警戒线之上的trace是安全的，之下的是不安全的
   * 初值设置任意值设置为MIN_VALUE，意味着初始时刻share trace buffer中的trace都是不安全的 cordonLine的最终状态为MAX_VALUE，以为这个share
   * trace buffer种的trace都是安全的
   */
  @Setter @Getter protected long cordonLine = Long.MIN_VALUE;

  /** Share trace buffer中唯一的sort thread,用于给share trace buffer补充trace */
  protected SortThread sortThread;

  public ShareTraceBuffer() {
    shareBuffer = new LinkedList<>();

    PrivateTraceBufferList.initialize();

    sortThread = new SortThread();
  }

  /**
   * 按照insertingOperationTrace的开始时间戳按序插入到buffer中
   *
   * @param insertingOperationTrace 需要插入buffer的trace
   */
  public void addTrace(OperationTrace insertingOperationTrace) {
    // debug
    footprintShareBuffer();

    ListIterator<OperationTrace> listIterator = shareBuffer.listIterator();

    OperationTrace nextTrace;
    while (listIterator.hasNext()) {
      nextTrace = listIterator.next();

      if (nextTrace.getStartTimestamp() < insertingOperationTrace.getStartTimestamp()) {
        listIterator.previous();
        break;
      }
    }
    listIterator.add(insertingOperationTrace);
  }

  /**
   * 查看尾部的Trace，如果移除尾部trace是不安全的，那么需要调用sort thread调度新的trace加入share trace buffer，从而下推警戒线
   *
   * @return 返回一条安全的trace,只有当所有的share trace buffer中所有的trace都分析完毕时，才会返回null
   */
  public OperationTrace peekLastTrace() {
    OperationTrace operationTrace = shareBuffer.peekLast();

    // 1.第一种情况，当ShareTraceBuffer头部为空且不是因为没有trace导致(即所有trace分析完毕)的时候，需要调用sort thread
    // 2.第二种情况，当ShareTraceBuffer头部的开始时间大于警戒线时，需要调用sort thread下推警戒线
    while ((operationTrace == null && OrcaVerify.shareTraceBuffer.getCordonLine() != Long.MAX_VALUE)
        || operationTrace != null
            && operationTrace.getStartTimestamp() >= OrcaVerify.shareTraceBuffer.getCordonLine()) {

      sortThread.run();
      operationTrace = shareBuffer.peekLast();
    }

    // 返回的一定是警戒线之上的安全的trace
    return operationTrace;
  }

  /** 从尾部移除最后一条trace */
  public void removeTrace() {
    OperationTrace trace = shareBuffer.removeLast();
    if (trace == null) {
      throw new RuntimeException("last trace must not be null");
    }
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return 类里面的全部对象
   */
  public Object[] getAllObject() {
    return new Object[] {shareBuffer, cordonLine, sortThread};
  }

  /** debug */
  protected int maxHeapSize = -50;

  public void footprintShareBuffer() {
    long startTS = System.nanoTime();
    // OutputStructure.outputStructure("*" + shareBuffer.size());
    if (shareBuffer.size() >= Config.PRIVATE_BUFFER_SIZE * Config.NUMBER_THREAD
        && shareBuffer.size() > maxHeapSize + 50) {
      maxHeapSize = shareBuffer.size();
      // OutputStructure.outputStructure(RamUsageEstimator.sizeOf(OrcaVerify.shareTraceBuffer.getAllObject()));
    }
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseDebugTime(finishTS - startTS);
  }
}
