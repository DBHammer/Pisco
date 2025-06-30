package ana.output;

import ana.main.OrcaVerify;
import java.util.*;

public class ErrorStatistics {
  /** error统计 */
  public static Map<ErrorType, Set<String>> errorStatistics = null;

  public static boolean triggerBug = false;

  /**
   * 判断是否存在至少一个和原始错误相同的错误
   *
   * @return 存在一个相同问题输出true
   */
  public static boolean hasSameError() {
    boolean hasSameError = false;
    //    errorStatistics.put(ErrorType.READ_OTHER_ERROR, new HashSet<>());
    for (ErrorType err : ErrorType.values()) {
      // 判断是否是同一种错误
      if (!errorStatistics.get(err).isEmpty() && originalErrorTypes.contains(err)) {
        // 判断错误的事物是不是同一个
        for (String id : errorStatistics.get(err)) {
          if (originalErrorTransactions.contains(id)) {
            hasSameError = true;
            break;
          }
        }
      }
    }

    return hasSameError;
  }

  public enum ErrorType {
    RECORD_LOCK_ERROR,
    PREDICATE_LOCK_ERROR,
    READ_SELF_ERROR,
    READ_OTHER_ERROR,
    SERIALIZE_ACCESS_ERROR,
    DEPENDENCY_CYCLE_ERROR
  }

  public static final Set<ErrorType> originalErrorTypes = new HashSet<>();
  public static final Set<String> originalErrorTransactions = new HashSet<>();

  public static void initialize() {
    errorStatistics = new HashMap<>();
    for (ErrorType err : ErrorType.values()) {
      errorStatistics.put(err, new HashSet<>());
    }
  }

  public static void increase(ErrorType errorType, String opId) {
    errorStatistics.get(errorType).add(opId);
  }

  /** 输出测试结果的分类统计 */
  public static void outputStatistics() {
    List<Integer> errorStatisticsList = new ArrayList<>();
    for (ErrorType err : ErrorType.values()) {
      errorStatisticsList.add(errorStatistics.get(err).size());
      OrcaVerify.logger.info("{}={}", err.toString(), errorStatistics.get(err).size());
    }

    OrcaVerify.logger.info("End of Verification: Error Statistics{}", errorStatisticsList);
  }

  public static boolean hasError() {
    int sum = 0;
    //    errorStatistics.put(ErrorType.READ_OTHER_ERROR, new HashSet<>());
    //    errorStatistics.put(ErrorType.READ_SELF_ERROR, new HashSet<>());
    for (Set<String> ids : errorStatistics.values()) {
      sum += ids.size();
    }
    return sum > 0;
    //    return sum > 0;
  }

  // 实现一个函数pinError,将errorStatistics中不为0的key放进originalErrorTypes中
  public static void pinError() {
    originalErrorTypes.clear();
    originalErrorTransactions.clear();

    for (ErrorType err : ErrorType.values()) {
      if (!errorStatistics.get(err).isEmpty()) {
        originalErrorTypes.add(err);
        originalErrorTransactions.addAll(errorStatistics.get(err));
      }
    }
  }
}
