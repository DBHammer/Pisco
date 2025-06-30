package dbloader.transaction.predicate;

import java.util.*;

public class PredicateChecker {
  /**
   * 检查一个Predicate是否有效 找到第一个 or 划分点，分而治之
   *
   * @param predList predList
   * @param connectors connectors
   * @return isValid
   */
  public static boolean check(List<PredGeneric> predList, List<String> connectors) {
    if (connectors == null || connectors.isEmpty()) return _check(predList);

    int divider = connectors.indexOf("or");

    if (divider < 0) return _check(predList);

    // 借助subList创建新的ArrayList，从而使下一层调用者能自由地排序
    return _check(predList.subList(0, divider + 1))
        || check(
            predList.subList(divider + 1, predList.size()),
            connectors.subList(divider + 1, connectors.size()));
  }

  /**
   * 检查一个 and 连接的式子
   *
   * @param predList and 连接的 Predicate
   * @return isValid
   */
  private static boolean _check(List<PredGeneric> predList) {
    // 创建新的 ArrayList，并按照key排序
    predList = new ArrayList<>(predList);
    predList.sort(Comparator.comparing(PredGeneric::getKey));

    // 创建一个随机的初始key，避免和真实的key冲突
    String previousKey = UUID.randomUUID().toString();
    Set<Integer> previousSet = null;

    for (PredGeneric pred : predList) {
      if (pred.getKey().equals(previousKey)) {
        previousSet.retainAll(pred.toSet());
      } else {
        previousKey = pred.getKey();
        previousSet = pred.toSet();
      }

      if (previousSet.isEmpty()) {
        return false;
      }
    }
    return true;
  }
}
