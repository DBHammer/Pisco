package ana.thread;

import ana.main.OrcaVerify;
import ana.window.AnalysisWindow;
import replay.controller.ReplayController;
import trace.OperationTrace;
import trace.OperationTraceType;

/**
 * dispatch线程会调度相关的trace到analysis window，每次调度一条OperationTrace
 *
 * @author like_
 */
public class DispatchThread implements Runnable {
  private boolean isOver;

  public DispatchThread() {
    this.isOver = false;
  }

  public boolean isOver() {
    return isOver;
  }

  @Override
  public void run() {

    // 1.从share operationTrace buffer中取出一条trace，并移除
    long startTS = System.nanoTime();
    OperationTrace operationTrace = OrcaVerify.shareTraceBuffer.peekLastTrace();
    if (operationTrace == null) {
      // 说明所有的trace都已经被调度到analysis window，share trace buffer完成了使命
      isOver = true;
      return;
    }
    OrcaVerify.shareTraceBuffer.removeTrace();
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseBufferModel(finishTS - startTS);

    // 2.将trace加入到analysis window中，并同步维护相关结构
    startTS = System.nanoTime();
    if (operationTrace.getOperationTraceType() != OperationTraceType.DDL
        && operationTrace.getOperationTraceType() != OperationTraceType.DistributeSchedule
        && operationTrace.getOperationTraceType() != OperationTraceType.FAULT) {
      AnalysisWindow.addTrace(operationTrace);
      ReplayController.sequenceDependency.addTrace(operationTrace);
    }
    if (operationTrace.getOperationTraceType() == OperationTraceType.DDL) {
      ReplayController.sequenceDependency.addTrace(operationTrace);
    }

    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increasePreparePurgeContext(finishTS - startTS);
  }
}
