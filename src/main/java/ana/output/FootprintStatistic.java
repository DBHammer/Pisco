package ana.output;

import ana.buffer.PrivateTraceBufferList;
import ana.graph.DependencyGraph;
import ana.main.OrcaVerify;
import ana.version.HistoryVersion;
import ana.window.Active;
import ana.window.AnalysisWindow;
import ana.window.WriteSet;
import ana.window.profile.ProfileMap;
import java.util.concurrent.CountDownLatch;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;

@Data
public class FootprintStatistic {

  private static final Logger logger = LogManager.getLogger(FootprintStatistic.class);

  private long shareTraceBuffer;
  private long privateTraceBufferList;
  private long bufferModule;

  private long analysisWindow;
  private long historyVersion;
  private long active;
  private long writeSet;
  private long profileMap;
  private long dependencyGraph;
  private long analysisModule;

  public void average(int counter) {
    this.bufferModule = this.bufferModule / counter;
    this.shareTraceBuffer = this.shareTraceBuffer / counter;
    this.privateTraceBufferList = this.privateTraceBufferList / counter;
    this.analysisModule = this.analysisModule / counter;
    this.analysisWindow = this.analysisWindow / counter;
    this.historyVersion = this.historyVersion / counter;
    this.active = this.active / counter;
    this.writeSet = this.writeSet / counter;
    this.profileMap = this.profileMap / counter;
    this.dependencyGraph = this.dependencyGraph / counter;
  }

  public static FootprintStatisticHuman outputFootprintStatistic(
      FootprintStatistic sumFootprintStatistic, FootprintStatistic maxFootprintStatistic) {
    FootprintStatistic fs = new FootprintStatistic();

    CountDownLatch cdl = new CountDownLatch(2);

    // 1.fork一个线程计算ShareTraceBuffer,PrivateTraceBufferList
    new Thread(
            () -> {
              // 1.1计算
              fs.setShareTraceBuffer(
                  RamUsageEstimator.sizeOf(OrcaVerify.shareTraceBuffer.getAllObject()));
              fs.setPrivateTraceBufferList(
                  RamUsageEstimator.sizeOf(PrivateTraceBufferList.getAllObject()));
              fs.setBufferModule(fs.getPrivateTraceBufferList() + fs.getShareTraceBuffer());

              // 1.2统计
              sumFootprintStatistic.setShareTraceBuffer(
                  fs.getShareTraceBuffer() + sumFootprintStatistic.getShareTraceBuffer());
              sumFootprintStatistic.setPrivateTraceBufferList(
                  fs.getPrivateTraceBufferList()
                      + sumFootprintStatistic.getPrivateTraceBufferList());
              sumFootprintStatistic.setBufferModule(
                  fs.getBufferModule() + sumFootprintStatistic.getBufferModule());

              // 1.3统计
              maxFootprintStatistic.setShareTraceBuffer(
                  Math.max(fs.getShareTraceBuffer(), maxFootprintStatistic.getShareTraceBuffer()));
              maxFootprintStatistic.setPrivateTraceBufferList(
                  Math.max(
                      fs.getPrivateTraceBufferList(),
                      maxFootprintStatistic.getPrivateTraceBufferList()));
              maxFootprintStatistic.setBufferModule(
                  Math.max(fs.getBufferModule(), maxFootprintStatistic.getBufferModule()));

              cdl.countDown();
            })
        .start();

    // 2.fork一个线程计算AnalysisWindow,HistoryVersion,ProfileMap,Active,WriteSet
    new Thread(
            () -> {
              // 2.1计算
              fs.setAnalysisWindow(RamUsageEstimator.sizeOf(AnalysisWindow.getAllObject()));
              fs.setHistoryVersion(RamUsageEstimator.sizeOf(HistoryVersion.getAllObject()));
              fs.setActive(RamUsageEstimator.sizeOf(Active.getAllObject()));
              fs.setWriteSet(RamUsageEstimator.sizeOf(WriteSet.getAllObject()));
              fs.setProfileMap(RamUsageEstimator.sizeOf(ProfileMap.getAllObject()));
              fs.setDependencyGraph(RamUsageEstimator.sizeOf(DependencyGraph.getAllObject()));
              fs.setAnalysisModule(
                  fs.getAnalysisWindow()
                      + fs.getHistoryVersion()
                      + fs.getActive()
                      + fs.getWriteSet()
                      + fs.getProfileMap()
                      + fs.getDependencyGraph());

              // 2.3统计
              sumFootprintStatistic.setAnalysisWindow(
                  fs.getAnalysisWindow() + sumFootprintStatistic.getAnalysisWindow());
              sumFootprintStatistic.setHistoryVersion(
                  fs.getHistoryVersion() + sumFootprintStatistic.getHistoryVersion());
              sumFootprintStatistic.setActive(fs.getActive() + sumFootprintStatistic.getActive());
              sumFootprintStatistic.setWriteSet(
                  fs.getWriteSet() + sumFootprintStatistic.getWriteSet());
              sumFootprintStatistic.setProfileMap(
                  fs.getProfileMap() + sumFootprintStatistic.getProfileMap());
              sumFootprintStatistic.setDependencyGraph(
                  fs.getDependencyGraph() + sumFootprintStatistic.getDependencyGraph());
              sumFootprintStatistic.setAnalysisModule(
                  fs.getAnalysisModule() + sumFootprintStatistic.getAnalysisModule());

              // 2.3统计
              maxFootprintStatistic.setAnalysisWindow(
                  Math.max(fs.getAnalysisWindow(), maxFootprintStatistic.getAnalysisWindow()));
              maxFootprintStatistic.setHistoryVersion(
                  Math.max(fs.getHistoryVersion(), maxFootprintStatistic.getHistoryVersion()));
              maxFootprintStatistic.setActive(
                  Math.max(fs.getActive(), maxFootprintStatistic.getActive()));
              maxFootprintStatistic.setWriteSet(
                  Math.max(fs.getWriteSet(), maxFootprintStatistic.getWriteSet()));
              maxFootprintStatistic.setProfileMap(
                  Math.max(fs.getProfileMap(), maxFootprintStatistic.getProfileMap()));
              maxFootprintStatistic.setDependencyGraph(
                  Math.max(fs.getDependencyGraph(), maxFootprintStatistic.getDependencyGraph()));
              maxFootprintStatistic.setAnalysisModule(
                  Math.max(fs.getAnalysisModule(), maxFootprintStatistic.getAnalysisModule()));

              cdl.countDown();
            })
        .start();

    // 3.等待上述两个线程计算结束
    try {
      cdl.await();
    } catch (InterruptedException e) {
      logger.warn(e);
    }

    return new FootprintStatisticHuman(fs);
  }
}
