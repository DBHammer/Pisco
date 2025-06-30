package ana.output;

import lombok.Data;

@Data
public class RuntimeStatistic {
  /** 整个java程序实际运行的时间 */
  private long userRealTime;

  /** 验证程序总体的运行时间 */
  private long analysisTotalTime;

  /** 缓冲模块调度Trace花费的时间 */
  private long bufferModel;

  /** 将Trace安装到上下文的时间 */
  private long preparePurgeContext;

  /** 开展锁互斥所花费的时间 */
  private long mutualExclusive;

  /** 开展读一致性验证所花费的时间 */
  private long readConsistency;

  /** 开展first-updater-wins所花费的时间 */
  private long firstUpdaterWins;

  /** 依赖图相关算法所花费的时间 */
  private long dependencyGraph;

  public void initialUserRealTime() {
    this.userRealTime = System.nanoTime();
  }

  public void setUserRealTime() {
    this.userRealTime = System.nanoTime() - this.userRealTime;
  }

  public void increaseBufferModel(long increment) {
    this.bufferModel += increment;
  }

  public void increasePreparePurgeContext(long increment) {
    this.preparePurgeContext += increment;
  }

  public void increaseMutualExclusive(long increment) {
    this.mutualExclusive += increment;
  }

  public void increaseReadConsistency(long increment) {
    this.readConsistency += increment;
  }

  public void increaseFirstUpdaterWins(long increment) {
    this.firstUpdaterWins += increment;
  }

  public void increaseDependencyGraph(long increment) {
    this.dependencyGraph += increment;
  }

  private long while1;

  public void increaseWhile1(long increment) {
    this.while1 += increment;
  }

  private long verify;

  public void increaseVerify(long increment) {
    this.verify += increment;
  }

  private long clear;

  public void increaseClear(long increment) {
    this.clear += increment;
  }

  private long AWPrepare;

  public void increaseAWPrepare(long increment) {
    this.AWPrepare += increment;
  }

  private long active1;

  public void increaseActive1(long increment) {
    this.active1 += increment;
  }

  private long profileMap1;

  public void increaseProfileMap1(long increment) {
    this.profileMap1 += increment;
  }

  private long DG1;

  public void increaseDG1(long increment) {
    this.DG1 += increment;
  }

  private long profileMap2;

  public void increaseProfileMap2(long increment) {
    this.profileMap2 += increment;
  }

  private long HV1;

  public void increaseHV1(long increment) {
    this.HV1 += increment;
  }

  private long DG2;

  public void increaseDG2(long increment) {
    this.DG2 += increment;
  }

  private long ProfileMap3;

  public void increaseProfileMap3(long increment) {
    this.ProfileMap3 += increment;
  }

  private long HV2;

  public void increaseHV2(long increment) {
    this.HV2 += increment;
  }

  private long HV11;

  public void increaseHV11(long increment) {
    this.HV11 += increment;
  }

  private long HV12;

  public void increaseHV12(long increment) {
    this.HV12 += increment;
  }

  private long HV13;

  public void increaseHV13(long increment) {
    this.HV13 += increment;
  }

  private long AddVersion1;

  public void increaseAddVersion1(long increment) {
    this.AddVersion1 += increment;
  }

  private long AddVersion2;

  public void increaseAddVersion2(long increment) {
    this.AddVersion2 += increment;
  }

  private long AddVersion3;

  public void increaseAddVersion3(long increment) {
    this.AddVersion3 += increment;
  }

  private long AddVersion4;

  public void increaseAddVersion4(long increment) {
    this.AddVersion4 += increment;
  }

  private long GetInitialVersion;

  public void increaseGetInitialVersion(long increment) {
    this.GetInitialVersion += increment;
  }

  private long trackWW;

  public void increaseTrackWW(long increment) {
    this.trackWW += increment;
  }

  private long trackWR;

  public void increaseTrackWR(long increment) {
    this.trackWR += increment;
  }

  private long CRS1;

  public void increaseCRS1(long increment) {
    this.CRS1 += increment;
  }

  private long CRS2;

  public void increaseCRS2(long increment) {
    this.CRS2 += increment;
  }

  private long CRS3;

  public void increaseCRS3(long increment) {
    this.CRS3 += increment;
  }

  private long CRS4;

  public void increaseCRS4(long increment) {
    this.CRS4 += increment;
  }

  private long debugTime;

  public void increaseDebugTime(long increment) {
    this.debugTime += increment;
  }

  private long pruningGraph;

  public void increasePruningGraph(long increment) {
    this.pruningGraph += increment;
  }

  private long addDependency;

  public void increaseAddDependency(long increment) {
    this.addDependency += increment;
  }
}
