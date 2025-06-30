package ana.buffer;

import ana.main.Config;
import ana.main.OrcaVerify;
import ana.thread.SortThread;
import java.util.Comparator;
import java.util.PriorityQueue;
import trace.OperationTrace;

public class ShareTraceBufferHeap extends ShareTraceBuffer {
  /** 核心数据结构：缓存所有trace file中的trace，按照小根堆方式组织，堆顶时间的trace开始时间最小 */
  private final PriorityQueue<OperationTrace> shareBuffer;

  public ShareTraceBufferHeap() {
    Comparator<? super OperationTrace> comparator =
        Comparator.comparing(OperationTrace::getStartTimestamp);

    // 创建一个指定大小的根堆，底层数组初始大小设为最大可能大小的60%
    shareBuffer =
        new PriorityQueue<>(
            (int) (0.6 * Config.NUMBER_THREAD * Config.PRIVATE_BUFFER_SIZE), comparator);

    PrivateTraceBufferList.initialize();

    sortThread = new SortThread();
  }

  /**
   * 按照insertingOperationTrace的开始时间戳按序插入到buffer中
   *
   * @param insertingOperationTrace insert operation trace
   */
  public void addTrace(OperationTrace insertingOperationTrace) {
    // debug
    footprintShareBuffer();

    shareBuffer.add(insertingOperationTrace);
  }

  /**
   * 查看尾部的Trace，如果移除尾部trace是不安全的，那么需要调用sort thread调度新的trace加入share trace buffer，从而下推警戒线
   *
   * @return 返回一条安全的trace,只有当所有的share trace buffer中所有的trace都分析完毕时，才会返回null
   */
  public OperationTrace peekLastTrace() {
    OperationTrace operationTrace = shareBuffer.peek();

    // 1.第一种情况，当ShareTraceBuffer头部为空且不是因为没有trace导致(即所有trace分析完毕)的时候，需要调用sort thread
    // 2.第二种情况，当ShareTraceBuffer头部的开始时间大于警戒线时，需要调用sort thread下推警戒线
    while ((operationTrace == null && OrcaVerify.shareTraceBuffer.getCordonLine() != Long.MAX_VALUE)
        || operationTrace != null
            && operationTrace.getStartTimestamp() >= OrcaVerify.shareTraceBuffer.getCordonLine()) {

      sortThread.run();
      operationTrace = shareBuffer.peek();
    }

    // 返回的一定是警戒线之上的安全的trace
    return operationTrace;
  }

  /** 从尾部移除最后一条trace */
  public void removeTrace() {
    OperationTrace trace = shareBuffer.poll();
    if (trace == null) {
      throw new RuntimeException("last trace must not be null");
    }
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return all object instances for memory statistics
   */
  public Object[] getAllObject() {
    return new Object[] {shareBuffer, cordonLine, sortThread};
  }

  /** debug */
  public void footprintShareBuffer() {
    // OutputStructure.outputStructure("*" + shareBuffer.size());
    if (shareBuffer.size() >= 0.85 * 2 * Config.PRIVATE_BUFFER_SIZE * Config.NUMBER_THREAD
        && shareBuffer.size() > maxHeapSize + 50) {
      maxHeapSize = shareBuffer.size();
      // OutputStructure.outputStructure(RamUsageEstimator.sizeOf(OrcaVerify.shareTraceBuffer.getAllObject()));
    }
    // OrcaVerify.runtimeStatistic.increaseDebugTime(finishTS - startTS);
  }
}
