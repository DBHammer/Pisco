package ana.buffer;

import ana.io.HandlerFactory;
import ana.main.Config;
import java.util.ArrayDeque;
import trace.OperationTrace;

/**
 * 每一个Thread私有的Trace Buffer
 *
 * @author like_
 */
public class PrivateTraceBuffer {

  /** 核心数据结构：缓存从file input的trace信息 */
  private final ArrayDeque<OperationTrace> privateBuffer;

  public PrivateTraceBuffer() {
    super();
    this.privateBuffer = new ArrayDeque<>(Config.PRIVATE_BUFFER_SIZE);
  }

  /**
   * input指定file的若干个trace到填充buffer中
   *
   * @param fileID file identifier for reading
   */
  public void fillBuffer(int fileID) {
    OperationTrace operationTrace;
    for (int i = 0; i < Config.PRIVATE_BUFFER_SIZE; i++) {
      operationTrace = HandlerFactory.nextOperationTrace(fileID);
      if (operationTrace == null) {
        return;
      }
      privateBuffer.addLast(operationTrace);
    }
  }

  public OperationTrace removeTrace() {
    return privateBuffer.pollFirst();
  }

  /**
   * 返回当前buffer的警戒线,如果没有trace，则返回MAX_VALUE，表示该buffer任何一个trace都可以读取
   *
   * @return cordon line
   */
  public long peekCordonLine() {
    if (privateBuffer.peekFirst() == null) {
      return Long.MAX_VALUE;
    }
    return privateBuffer.peekFirst().getStartTimestamp();
  }

  /**
   * 根据 当前线程id 加载所有的 OperationTrace
   *
   * @param fileID 文件id，即线程id
   */
  public int loadAllTrace(int fileID) {
    OperationTrace operationTrace;

    while (true) {
      operationTrace = HandlerFactory.nextOperationTrace(fileID);
      if (operationTrace == null) {
        return privateBuffer.size();
      }
      privateBuffer.addLast(operationTrace);
    }
  }
}
