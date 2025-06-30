package ana.version;

import ana.graph.Dependency;
import ana.main.Config;
import ana.main.OrcaVerify;
import ana.thread.AnalysisThread;
import ana.window.Active;
import ana.window.WriteSet;
import java.util.HashMap;
import lombok.Getter;

/**
 * 封装所有数据的version
 *
 * @author like_
 */
public class HistoryVersion {

  /** 核心数据结构 key:数据的唯一标识 value:该key对应的version chain */
  @Getter private static HashMap<String, VersionChain> historyVersion;

  public static void initialize() {
    WriteSet.initialize();

    historyVersion = new HashMap<>();

    // init version

  }

  /**
   * 在指定的versionChain(key)中添加一个新version（是已提交版本或者未提交版本），并完成version chain的清理工作
   *
   * @param key key of data
   * @param newVersion version of this data
   */
  public static void addVersion(String key, Version newVersion) {
    VersionChain versionChain = historyVersion.get(key);

    // 1.如果对应的version chain不存在，那么新new一个
    long startTS = System.nanoTime();
    if (versionChain == null) {
      versionChain = new VersionChain(key);
      historyVersion.put(key, versionChain);
    }
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAddVersion1(finishTS - startTS);

    // 2.将新的version添加到version chain中
    startTS = System.nanoTime();
    versionChain.addVersion(newVersion);
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAddVersion2(finishTS - startTS);

    // 3.如果新添加的版本是一个已提交版本，那么跟踪WW依赖，其他类型的版本不会参与且不会影响WW依赖的构建
    startTS = System.nanoTime();
    if (newVersion.getStatus() == VersionStatus.COMMITTED) {
      Dependency.trackWW(key, versionChain);
    }
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAddVersion3(finishTS - startTS);

    // 4.如果versionChain的长度超过一定长度，那么就发起一个清理操作
    startTS = System.nanoTime();
    if (Config.LAUNCH_GC && versionChain.getLength() >= Config.VERSION_CHAIN_PURGE_LENGTH) {
      // 4.1获取Earliest Consistent Read Time Interval，清理version chain
      Version earliestConsistentReadTimeInterval = Active.getEarliestConsistentReadTimeInterval();
      // 应该以已提交版本为基准进行清理，
      AnalysisThread.getCandidateReadSet(
          key, earliestConsistentReadTimeInterval, true, true, false);
    }
    finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAddVersion4(finishTS - startTS);
  }

  /**
   * 获取指定key的version chain
   *
   * @return chain of the key
   */
  public static VersionChain getVersionChain(String key) {
    VersionChain versionChain = historyVersion.get(key);
    if (versionChain == null) {
      // 如果对应的version chain不存在则new一个，放入historyVersion中
      versionChain = new VersionChain(key);
      historyVersion.put(key, versionChain);
    }

    return versionChain;
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return object themselves
   */
  public static Object[] getAllObject() {
    return new Object[] {historyVersion};
  }
}
