package ana.graph;

import ana.main.Config;
import ana.main.OrcaVerify;
import ana.output.ErrorStatistics;
import ana.output.OutputCertificate;
import ana.output.OutputStructure;
import ana.version.Version;
import ana.window.Active;
import ana.window.WriteSet;
import ana.window.profile.Profile;
import ana.window.profile.ProfileMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jgrapht.graph.SimpleDirectedGraph;

public class DependencyGraph {
  /** 核心数据结构：一个简单有向图，图的节点是Profile，图的边是依赖 */
  private static SimpleDirectedGraph<Profile, Dependency> dependencyGraph;

  public static void initialize() {
    dependencyGraph = new SimpleDirectedGraph<>(Dependency.class);
  }

  /**
   * 向依赖图中添加一个事务Profile，当事务节点数超过一定数目，就发起清理算法
   *
   * @param profile 需要添加的事务节点
   */
  public static void addProfile(Profile profile) {
    dependencyGraph.addVertex(profile);

    if (Config.LAUNCH_GC && dependencyGraph.vertexSet().size() >= Config.GRAPH_PURGE_SIZE) {
      pruningGraph();
    }
  }

  /** 依赖图的清理算法，即找到事务提交时间戳在最早crti开始时间戳之前且入度为0的事务 */
  private static void pruningGraph() {
    long startTS = System.nanoTime();

    // 1.获取最早读一致性时间区间
    Version earliestConsistentReadTimeInterval = Active.getEarliestConsistentReadTimeInterval();

    // 2.找到需要剪枝的事务节点
    ArrayList<Profile> pruningProfile = new ArrayList<>();
    for (Profile profile : dependencyGraph.vertexSet()) {
      // 2.1可以被清理的事务Profile需要满足下述四个条件
      // 2.1.1其不是一个活跃的事务，即事务的所有trace都已经被分析过了，那么其所有的读操作都经过了WR依赖的判断（WR入边依赖），不可能有新的WR依赖产生
      // 2.1.2其结束操作的结束时间戳完全在最早读一致性时间区间开始时间戳之前，那么其所有的写操作都经过了RW依赖的判断（RW入边依赖），不可能有新的RW依赖产生
      // 2.1.3其写入的所有Version都已经与其前驱考虑过WW依赖了，那么其所有的写操作都经过了了WW依赖的判断（WW入边依赖），不可能有新的WW依赖产生
      // 2.1.4其入度为0，即不可能有新的入边产生且此时入度为0，那么该事务永远不可能构成环路，所以可以被清理
      if (!Active.isActive(profile.getTransactionID())
          && profile.getEndFinishTimestamp()
              < earliestConsistentReadTimeInterval.getStartTimestamp()
          && WriteSet.allIsWW(WriteSet.getHistoryVersion(), profile.getTransactionID())
          && dependencyGraph.inDegreeOf(profile) == 0) {
        pruningProfile.add(profile);
      }
    }

    // 3.移除可以剪枝的事务节点
    for (Profile profile : pruningProfile) {
      dependencyGraph.removeVertex(profile);
    }

    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increasePruningGraph(finishTS - startTS);
  }

  /**
   * 在依赖图中找到一个指定的事务节点
   *
   * @param transactionID 事务id
   * @return 对应事务的具体信息
   */
  public static Profile getProfile(String transactionID) {
    // 1.优化：为了降低搜索Profile的代价，第一步先在ProfileMap中通过Hash找到指定的profile
    Profile tempProfile = ProfileMap.getProfile(transactionID);
    if (tempProfile != null) {
      if (dependencyGraph.containsVertex(tempProfile)) {
        return tempProfile;
      } else {
        return null;
      }
    }

    // 2.如果在profileMap中找不到，那么启动全图搜索
    for (Profile profile : dependencyGraph.vertexSet()) {
      if (profile.getTransactionID().equals(transactionID)) {
        return profile;
      }
    }
    return null;
  }

  /**
   * 在依赖图中移除一个移除一个指定的事务
   *
   * @param transactionID 需要移除的事务
   * @throws NullPointerException 可能要被移除的事务节点在依赖图中不存在
   */
  public static void removeProfile(String transactionID) {
    dependencyGraph.removeVertex(getProfile(transactionID));
  }

  /**
   * 在两个事务节点之间添加一条指定的边，如果该边已经存在，那么追加新的依赖类型到依赖边中
   *
   * @param transactionID1 第一个事务
   * @param transactionID2 第二个事务
   * @param newDependency 依赖类型
   * @param dependencyKey 依赖键
   * @param fromOperationID 依赖的操作来源
   * @param toOperationID 依赖的目标操作
   */
  public static void addDependency(
      String transactionID1,
      String transactionID2,
      String newDependency,
      String dependencyKey,
      String fromOperationID,
      String toOperationID) {
    OutputStructure.outputStructure(
        "SC+" + newDependency + ":" + transactionID1 + "*" + transactionID2);

    long startTS1 = System.nanoTime();

    // 1.获取依赖边对应的节点
    Profile profile1 = DependencyGraph.getProfile(transactionID1);
    Profile profile2 = DependencyGraph.getProfile(transactionID2);

    // 2.如果对应的节点不存在那么放弃添加依赖边
    if (profile1 == null || profile2 == null) {
      return;
    }

    // 3.添加依赖边，如果已经有依赖边，那么将新的边融合到旧的依赖边中
    Dependency oldDependency = dependencyGraph.getEdge(profile1, profile2);
    if (oldDependency != null) {
      oldDependency.incorporate(newDependency);
      dependencyGraph.addEdge(profile1, profile2, oldDependency);
    } else {
      if (Config.PG_OPTIMIZATION) {
        // 3.1.1忽略非并行事务产生的依赖
        if (Math.max(profile1.getBeginStartTimestamp(), profile2.getBeginStartTimestamp())
            > Math.min(profile1.getEndFinishTimestamp(), profile2.getEndFinishTimestamp())) {
          return;
        }
      }
      // 3.1.2添加新依赖边
      dependencyGraph.addEdge(
          profile1,
          profile2,
          new Dependency(newDependency, dependencyKey, fromOperationID, toOperationID));

      // 3.1.3统计
      switch (newDependency) {
        case Dependency.WW:
          OrcaVerify.numberStatistic.increaseDependency(0);
          break;
        case Dependency.WR:
          OrcaVerify.numberStatistic.increaseDependency(1);
          break;
        case Dependency.RW:
          OrcaVerify.numberStatistic.increaseDependency(2);
          break;
      }

      if (Config.VERIFY) {
        // 3.2如果成功添加新依赖边，基于新添加的边发动一次环路搜索算法
        long startTS = System.nanoTime();
        if (!Config.PG_OPTIMIZATION) {
          // 3.2.1不开启环路搜索优化算法，直接暴力搜索环路
          JohnsonSimpleCycles<Profile, Dependency> johnsonSimpleCycles =
              new JohnsonSimpleCycles<>(dependencyGraph);
          if (!Config.CLOSE_CYCLE_DETECTION) {
            List<List<Profile>> cycles = johnsonSimpleCycles.findSimpleCycles(profile1);
            if (!cycles.isEmpty()) {
              for (int i = 0; i < cycles.size(); i++) {
                ErrorStatistics.increase(
                    ErrorStatistics.ErrorType.DEPENDENCY_CYCLE_ERROR, profile1.getTransactionID());
              }
              OutputCertificate.outputDependencyCycleError(cycles);
            }
          }
        } else {
          // 3.2.2开启环路搜索优化算法，搜索两条连续的RW依赖

          // 3.2.2.1当前新加入的是一个RW依赖
          if (newDependency.equals(Dependency.RW)) {

            // 3.2.2.2找到与当前依赖相邻的所有依赖
            Set<Dependency> profile1IncomingEdges = dependencyGraph.incomingEdgesOf(profile1);
            Set<Dependency> profile2OutgoingEdges = dependencyGraph.outgoingEdgesOf(profile2);

            // 3.2.2.3判断是否存在RW依赖
            for (Dependency dependency : profile1IncomingEdges) {
              for (String dependencyType : dependency.getDependencies()) {
                if (dependencyType.equals(Dependency.RW)) {
                  ErrorStatistics.increase(
                      ErrorStatistics.ErrorType.DEPENDENCY_CYCLE_ERROR,
                      profile1.getTransactionID());
                }
              }
            }
            for (Dependency dependency : profile2OutgoingEdges) {
              for (String dependencyType : dependency.getDependencies()) {
                if (dependencyType.equals(Dependency.RW)) {
                  ErrorStatistics.increase(
                      ErrorStatistics.ErrorType.DEPENDENCY_CYCLE_ERROR,
                      profile1.getTransactionID());
                }
              }
            }
          }
        }

        long finishTS = System.nanoTime();
        OrcaVerify.runtimeStatistic.increaseDependencyGraph(finishTS - startTS);
      }
    }

    long finishTS1 = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseAddDependency(finishTS1 - startTS1);
  }

  public static SimpleDirectedGraph<Profile, Dependency> getGraph() {
    return dependencyGraph;
  }

  /** 将图整理成指定格式 */
  public static class OutputGraph {
    // 节点集合
    private final Set<Profile> profileSet;
    // 边集合
    private final Set<Dependency> dependencySet;

    public OutputGraph(Set<Profile> profileSet, Set<Dependency> dependencySet) {
      this.profileSet = profileSet;
      this.dependencySet = dependencySet;
    }
  }

  /**
   * 将图的节点集合和边集合输出
   *
   * @return 需要输出的图结构，仅包含点和边的集合
   */
  public static OutputGraph outputGraph(SimpleDirectedGraph<Profile, Dependency> dependencyGraph) {
    Set<Dependency> dependencySet = new HashSet<>();

    for (Dependency dependency : dependencyGraph.edgeSet()) {
      dependency.setFromProfile(dependencyGraph.getEdgeSource(dependency));
      dependency.setToProfile(dependencyGraph.getEdgeTarget(dependency));
      dependencySet.add(dependency);
    }

    return new OutputGraph(dependencyGraph.vertexSet(), dependencySet);
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return 类的所有对象
   */
  public static Object[] getAllObject() {
    return new Object[] {dependencyGraph};
  }
}
