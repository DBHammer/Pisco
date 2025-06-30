package replay.controller.cascadeDetect;

import java.util.HashSet;
import java.util.Set;
import replay.controller.ReplayController;

public class CascadeDelete {
  // 已经确定需要被级联删除操作的集合: 删除这些操作，并不能触发bug
  static final Set<String> historySet = new HashSet<>();

  // 当前被级联删除操作的集合
  static final Set<String> currentSet = new HashSet<>();

  // new_Set add into historySet
  public static void addIntoHistory() {
    historySet.addAll(currentSet);
    currentSet.clear();
    // 持久化依赖图里的变化
    ReplayController.getVersionOperationGraph().persist();
  }

  // clear currentSet
  public static void clearNewSet() {
    currentSet.clear();
  }

  /**
   * add new cascade operationTraces into currentSet
   *
   * @param operationIDs 被删除的操作
   */
  public static void addIntoCurrentSet(Set<String> operationIDs) {

    Set<String> cascadeDeleteSet = new HashSet<>();
    for (String operationID : operationIDs) {
      cascadeDeleteSet.addAll(
          ReplayController.getVersionOperationGraph().updateDepend(operationID, operationIDs));
    }

    cascadeDeleteSet.addAll(operationIDs);
    currentSet.addAll(cascadeDeleteSet);
  }

  // check whether an operationTrace is in historySet and currentSet
  public static boolean isCascadeDelete(String operationID) {
    return historySet.contains(operationID) || currentSet.contains(operationID);
  }
}
