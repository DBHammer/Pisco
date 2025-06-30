package ana.output;

import java.text.NumberFormat;
import java.util.Locale;

public class RuntimeStatisticHuman {
  private final String userRealTime;

  private final String analysisTotalTime;

  private final String bufferModel;

  private final String preparePurgeContext;

  private final String mutualExclusive;

  private final String readConsistency;

  private final String firstUpdaterWins;

  private final String dependencyGraph;

  public RuntimeStatisticHuman(RuntimeStatistic runtimeStatistic) {
    NumberFormat usFormat = NumberFormat.getIntegerInstance(Locale.US);
    this.userRealTime = usFormat.format(runtimeStatistic.getUserRealTime());
    this.analysisTotalTime = usFormat.format(runtimeStatistic.getAnalysisTotalTime());
    this.bufferModel = usFormat.format(runtimeStatistic.getBufferModel());
    this.preparePurgeContext = usFormat.format(runtimeStatistic.getPreparePurgeContext());
    this.mutualExclusive = usFormat.format(runtimeStatistic.getMutualExclusive());
    this.readConsistency = usFormat.format(runtimeStatistic.getReadConsistency());
    this.firstUpdaterWins = usFormat.format(runtimeStatistic.getFirstUpdaterWins());
    this.dependencyGraph = usFormat.format(runtimeStatistic.getDependencyGraph());

    this.while1 = usFormat.format(runtimeStatistic.getWhile1());
    this.verify = usFormat.format(runtimeStatistic.getVerify());
    this.clear = usFormat.format(runtimeStatistic.getClear());

    this.AWPrepare = usFormat.format(runtimeStatistic.getAWPrepare());
    this.active1 = usFormat.format(runtimeStatistic.getActive1());
    this.profileMap1 = usFormat.format(runtimeStatistic.getProfileMap1());
    this.DG1 = usFormat.format(runtimeStatistic.getDG1());
    this.profileMap2 = usFormat.format(runtimeStatistic.getProfileMap2());
    this.HV1 = usFormat.format(runtimeStatistic.getHV1());
    this.DG2 = usFormat.format(runtimeStatistic.getDG2());
    this.ProfileMap3 = usFormat.format(runtimeStatistic.getProfileMap3());
    this.HV2 = usFormat.format(runtimeStatistic.getHV2());

    this.HV11 = usFormat.format(runtimeStatistic.getHV11());
    this.HV12 = usFormat.format(runtimeStatistic.getHV12());
    this.HV13 = usFormat.format(runtimeStatistic.getHV13());

    this.AddVersion1 = usFormat.format(runtimeStatistic.getAddVersion1());
    this.AddVersion2 = usFormat.format(runtimeStatistic.getAddVersion2());
    this.AddVersion3 = usFormat.format(runtimeStatistic.getAddVersion3());
    this.AddVersion4 = usFormat.format(runtimeStatistic.getAddVersion4());

    this.GetInitialVersion = usFormat.format(runtimeStatistic.getGetInitialVersion());

    this.trackWW = usFormat.format(runtimeStatistic.getTrackWW());
    this.trackWR = usFormat.format(runtimeStatistic.getTrackWR());

    this.CRS1 = usFormat.format(runtimeStatistic.getCRS1());
    this.CRS2 = usFormat.format(runtimeStatistic.getCRS2());
    this.CRS3 = usFormat.format(runtimeStatistic.getCRS3());
    this.CRS4 = usFormat.format(runtimeStatistic.getCRS4());

    this.debugTime = usFormat.format(runtimeStatistic.getDebugTime());

    this.pruningGraph = usFormat.format(runtimeStatistic.getPruningGraph());

    this.addDependency = usFormat.format(runtimeStatistic.getAddDependency());
  }

  private final String while1;

  private final String verify;

  private final String clear;

  private final String AWPrepare;

  private final String active1;

  private final String profileMap1;

  private final String DG1;

  private final String profileMap2;

  private final String HV1;

  private final String DG2;

  private final String ProfileMap3;

  private final String HV2;

  private final String HV11;

  private final String HV12;

  private final String HV13;

  private final String AddVersion1;

  private final String AddVersion2;

  private final String AddVersion3;

  private final String AddVersion4;

  private final String GetInitialVersion;

  private final String trackWW;

  private final String trackWR;

  private final String CRS1;

  private final String CRS2;

  private final String CRS3;

  private final String CRS4;

  private final String debugTime;

  private final String pruningGraph;

  private final String addDependency;
}
