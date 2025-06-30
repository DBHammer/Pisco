package ana.thread;

import static ana.main.OrcaVerify.logger;

import ana.buffer.PrivateTraceBuffer;
import ana.buffer.PrivateTraceBufferList;
import ana.main.Config;
import ana.main.OrcaVerify;
import java.util.concurrent.CountDownLatch;
import trace.OperationTrace;

/**
 * 轮询private trace buffer，将其中的所有trace插入排序到share trace buffer中，并更新警戒线
 *
 * @author like_
 */
public class SortThread implements Runnable {

  @Override
  public void run() {
    PrivateTraceBuffer privateTraceBuffer;
    OperationTrace trace;

    // 1.轮询每一个buffer，将其中的trace加入到share trace buffer中
    for (int bufferID = 0; bufferID < Config.NUMBER_THREAD; bufferID++) {
      privateTraceBuffer = PrivateTraceBufferList.getPrivateBuffer(bufferID);

      // 1.1将每一个buffer中的所有trace添加到Share trace buffer中
      while ((trace = privateTraceBuffer.removeTrace()) != null) {
        //				if (trace.getOperationTraceType() != OperationTraceType.DDL){ // 暂时忽略DDL的分析
        OrcaVerify.shareTraceBuffer.addTrace(trace);
        //				}
      }
    }

    // 2.调用异步IO Thread，将Trace从磁盘上input到private trace buffer
    CountDownLatch countDownLatch = new CountDownLatch(Config.NUMBER_THREAD);
    for (int ioThreadID = 0; ioThreadID < Config.NUMBER_THREAD; ioThreadID++) {
      // 2.1启动IO Thread
      new Thread(new IOThread(ioThreadID, countDownLatch)).start();
    }

    // 3.sort thread等待IO thread完成任务之后，才能结束
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      logger.warn(e);
    }

    long privateCordonLine;
    // 4.轮询每一个buffer的头部，找到最小的开始时间戳，更改警戒线
    OrcaVerify.shareTraceBuffer.setCordonLine(Long.MAX_VALUE);
    for (int bufferID = 0; bufferID < Config.NUMBER_THREAD; bufferID++) {
      privateTraceBuffer = PrivateTraceBufferList.getPrivateBuffer(bufferID);
      privateCordonLine = privateTraceBuffer.peekCordonLine();

      // 4.1选择所有privatebuffer中开始时间戳最小的作为cordon line,privatebufffer中的trace一定严格有序
      if (privateCordonLine < OrcaVerify.shareTraceBuffer.getCordonLine()) {
        OrcaVerify.shareTraceBuffer.setCordonLine(privateCordonLine);
      }
    }
    // 4.2if(ShareTraceBuffer.getCordonLine() == Long.MAX_VALUE)
    // 说明所有的trace都已经被input完毕并且放在了share trace buffer中
  }

  /**
   * 完成private trace buffer与trace file的交互
   *
   * @author like_
   */
  class IOThread implements Runnable {

    /** 自定义io thread id,即fileID，即bufferID ioThreadID、fileID、bufferID三者一一对应，描述不同的程序组件 */
    private final int ioThreadID;

    /** 与Sort Thread通信 */
    private final CountDownLatch countDownLatch;

    public IOThread(int ioThreadID, CountDownLatch countDownLatch) {
      super();
      this.ioThreadID = ioThreadID;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
      try {
        // 1.启动IO
        PrivateTraceBufferList.getPrivateBuffer(ioThreadID).fillBuffer(ioThreadID);
      } catch (OutOfMemoryError oom) {
        // 2.IO过程中可能会导致JVM内存溢出
        logger.warn("I/O Thread " + ioThreadID + " cause OOM");
        System.exit(-1);
      } finally {
        // 3.无论如何都要释放latch，通知Sort Thread
        this.countDownLatch.countDown();
      }
    }
  }
}
