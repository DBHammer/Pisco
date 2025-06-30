package ana.output;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.lucene.util.RamUsageEstimator;

@Data
public class FootprintStatisticHuman {
  @Setter @Getter private String shareTraceBuffer;
  @Setter @Getter private String privateTraceBufferList;
  @Setter @Getter private String bufferModule;

  @Setter @Getter private String analysisModule;
  @Setter @Getter private String analysisWindow;
  @Setter @Getter private String historyVersion;
  @Setter @Getter private String active;
  @Setter @Getter private String writeSet;
  @Setter @Getter private String profileMap;
  private String dependencyGraph;

  public FootprintStatisticHuman(FootprintStatistic footprintStatistic) {
    this.shareTraceBuffer =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getShareTraceBuffer());
    this.privateTraceBufferList =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getPrivateTraceBufferList());
    this.bufferModule = RamUsageEstimator.humanReadableUnits(footprintStatistic.getBufferModule());
    this.analysisModule =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getAnalysisModule());
    this.analysisWindow =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getAnalysisWindow());
    this.historyVersion =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getHistoryVersion());
    this.active = RamUsageEstimator.humanReadableUnits(footprintStatistic.getActive());
    this.writeSet = RamUsageEstimator.humanReadableUnits(footprintStatistic.getWriteSet());
    this.profileMap = RamUsageEstimator.humanReadableUnits(footprintStatistic.getProfileMap());
    this.dependencyGraph =
        RamUsageEstimator.humanReadableUnits(footprintStatistic.getDependencyGraph());
  }

  @Override
  public String toString() {
    return "FootprintStatisticHuman{"
        + "shareTraceBuffer='"
        + shareTraceBuffer
        + '\''
        + ", privateTraceBufferList='"
        + privateTraceBufferList
        + '\''
        + ", bufferModule='"
        + bufferModule
        + '\''
        + ", analysisModule='"
        + analysisModule
        + '\''
        + ", analysisWindow='"
        + analysisWindow
        + '\''
        + ", historyVersion='"
        + historyVersion
        + '\''
        + ", active='"
        + active
        + '\''
        + ", writeSet='"
        + writeSet
        + '\''
        + ", profileMap='"
        + profileMap
        + '\''
        + ", dependencyGraph='"
        + dependencyGraph
        + '\''
        + '}';
  }
}
