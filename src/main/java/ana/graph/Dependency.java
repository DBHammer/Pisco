package ana.graph;

import ana.main.Config;
import ana.main.OrcaVerify;
import ana.thread.AnalysisThread;
import ana.version.Version;
import ana.version.VersionChain;
import ana.window.profile.Profile;
import java.util.ArrayList;
import java.util.ListIterator;
import lombok.Getter;
import lombok.Setter;

public class Dependency {

  /** 辅助数据结构，保存有向边的两个顶点 */
  @Setter private Profile fromProfile;

  @Setter private Profile toProfile;

  /** 核心数据结构，保存事务之间的多种依赖关系 */
  @Getter private final ArrayList<String> dependencies;

  /**
   * 保存fromProfile和toProfile之间的第一个被构建的依赖信息 dependecnyKey：基于哪个key构建的依赖
   * fromOperationID：依赖边来源的OperationTraceID toOperationID：依赖边注入的OperationTraceID
   */
  @Getter private final String dependecnyKey;

  private final String fromOperationID;
  private final String toOperationID;

  public Dependency(
      String dependency, String dependecnyKey, String fromOperationID, String toOperationID) {
    this.dependencies = new ArrayList<>();
    this.dependencies.add(dependency);

    this.dependecnyKey = dependecnyKey;
    this.fromOperationID = fromOperationID;
    this.toOperationID = toOperationID;
  }

  /**
   * 将新依赖类型添加到旧依赖中，并且排除重复
   *
   * @param newDependency 加入新的依赖
   */
  public void incorporate(String newDependency) {
    if (!dependencies.contains(newDependency)) {
      dependencies.add(newDependency);
    }
  }

  /**
   * 在指定dependencyKey的Version Chain上检查是否产生写写依赖
   *
   * @param versionChain 版本链
   */
  public static void trackWW(String dependencyKey, VersionChain versionChain) {
    long startTS = System.nanoTime();

    ListIterator<Version> listIterator = versionChain.getIterator();

    // 1.直接后移一个Version，最后一个Version一定不能明确是否与其前一个Version构成WW依赖
    // 除非所有的Trace都已经调度完毕，那么最后一个Version具备开展WW依赖检查的条件
    if (!AnalysisThread.getDispatchThread().isOver()) {
      listIterator.next();
    }

    // 2.检查Version Chain中每一个version是否与其前一个Version形成WW依赖，只考虑已提交版本
    Version nextVersion;
    Version nextVersionPrecursor;
    while (VersionChain.hasNext(listIterator, true)) {
      nextVersion = listIterator.next();

      // 2.1如果nextVersion已经考察过与其前驱版本的WW依赖，那么该nextVersion之前的所有Version都已经考察过WW依赖了，结束遍历
      if (nextVersion.isWW()) {
        long finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseTrackWW(finishTS - startTS);
        return;
      }

      // 2.2考察是否与其前驱版本构成WW依赖
      if (VersionChain.hasNext(listIterator, true)) {
        // 2.2.1找到nextVersion的前驱版本，并将游标回退一个
        nextVersionPrecursor = listIterator.next();
        listIterator.previous();

        // 2.2.2 nextVersionPrecursor不能为-1,-1
        // 判断nextVersion和nextVersionPrecursor是否与VersionChain中的其他已提交Version重叠
        if (!nextVersionPrecursor.getTransactionID().equals("-1,-1")
            && versionChain.isOverlapping(nextVersion)
            && versionChain.isOverlapping(nextVersionPrecursor)) {
          // 2.2.2.1构建WW依赖
          if (Config.TRACK_WW) {
            DependencyGraph.addDependency(
                nextVersionPrecursor.getTransactionID(),
                nextVersion.getTransactionID(),
                WW,
                dependencyKey,
                nextVersionPrecursor.getProducerID(),
                nextVersion.getProducerID());
          }

          // 2.2.2.2设置前驱节点的后继
          nextVersionPrecursor.setSuccessor(nextVersion.getTransactionID());

          // 2.2.2.3在nextVersionPrecursor.readList与nextVersion之间构建RW依赖
          for (String readerID : nextVersionPrecursor.getReadList()) {
            // 2.2.2.3.1存在一种情况，RW依赖的读者和写者是同一个事务
            if (!readerID.equals(nextVersion.getTransactionID())) {
              if (Config.TRACK_RW) {
                DependencyGraph.addDependency(
                    readerID,
                    nextVersion.getTransactionID(),
                    RW,
                    dependencyKey,
                    readerID,
                    nextVersion.getProducerID());
              }
            }
          }
        }
      }

      // 2.3设置后继节点经过了检查
      nextVersion.setWW(true);
    }

    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseTrackWW(finishTS - startTS);
  }

  /**
   * 添加一条wr依赖，从writerID到readerID
   *
   * @param writerID WR依赖的写者
   * @param readerID WR依赖的读者
   * @param successor WR依赖的写者的后继
   */
  public static void trackWR(
      String writerID,
      String readerID,
      String successor,
      String dependencyKey,
      String fromOperationTraceID,
      String toOperationTraceID) {
    long startTS = System.nanoTime();

    // 1.构建从writerID到readerID之间的WR依赖
    if (Config.TRACK_WR) {
      DependencyGraph.addDependency(
          writerID, readerID, WR, dependencyKey, fromOperationTraceID, toOperationTraceID);
    }

    // 2.构建WR依赖的读者与WR依赖的写者的后继之间的RW依赖关系
    if (successor != null && !readerID.equals(successor)) { // 存在一种情况，WR依赖的读者与WR依赖的写者的后继是同一个事务
      if (Config.TRACK_RW) {
        DependencyGraph.addDependency(
            readerID, successor, RW, dependencyKey, toOperationTraceID, successor);
      }
    }

    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseTrackWR(finishTS - startTS);
  }

  public static final String WW = "ww";
  public static final String WR = "wr";
  public static final String RW = "rw";
}
