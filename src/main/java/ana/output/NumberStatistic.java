package ana.output;

import ana.main.OrcaVerify;
import trace.OperationTrace;

public class NumberStatistic {
  /** 整个workload一共分析了多少条trace */
  private long trace;

  /** 整个workload一共分析了多少个事务 0：提交事务 1：回滚事务 */
  private final long[] transaction;

  /** 整个workload一共读了多少个Tuple */
  private long readTuple;

  /** 整个workload一共写了多少个Tuple */
  private long writeTuple;

  /** History version中一共有多少个版本 0：未提交版本 1：回滚版本 2：已提交版本 3：初始版本 */
  private final long[] version;

  /** Dependency Graph中一共有多少依赖 0：WW依赖 1：WR依赖 2：RW依赖 */
  private final long[] dependency;

  /** 整个分析过程一共进行多少次所互斥验证 */
  private long mutualExclusive;

  /** 整个分析过程一共进行多少次first-updater-wins验证 */
  private long firstUpdaterWins;

  public NumberStatistic() {
    this.transaction = new long[2];
    this.version = new long[4];
    this.dependency = new long[3];
  }

  public void increaseTrace() {
    this.trace++;
  }

  public void increaseTransaction(int index) {
    this.transaction[index]++;
  }

  public void increaseReadTuple(int i) {
    this.readTuple += i;
  }

  public void increaseWriteTuple(int i) {
    this.writeTuple += i;
  }

  public void increaseVersion(int index) {
    this.version[index]++;
  }

  public void increaseDependency(int index) {
    this.dependency[index]++;
  }

  public void increaseMutualExclusive() {
    this.mutualExclusive++;
  }

  public void increaseFirstUpdaterWins() {
    this.firstUpdaterWins++;
  }

  /**
   * 依据operationTrace的类型，统计负载的特征
   *
   * @param operationTrace
   */
  public void statistic(OperationTrace operationTrace) {

    OrcaVerify.numberStatistic.increaseTrace();

    switch (operationTrace.getOperationTraceType()) {
      case START:
        OrcaVerify.logger.info(
            "T:" + operationTrace.getTransactionID() + " IL:" + operationTrace.getIsolationLevel());
        break;
      case COMMIT:
        OrcaVerify.numberStatistic.increaseTransaction(0);
        break;
      case ROLLBACK:
        OrcaVerify.numberStatistic.increaseTransaction(1);
        break;
      case INSERT:
      case DELETE:
      case UPDATE:
        if (operationTrace.getWriteTupleList() != null) {
          OrcaVerify.numberStatistic.increaseWriteTuple(operationTrace.getWriteTupleList().size());
        }
        break;
      case SELECT:
        if (operationTrace.getReadTupleList() == null) {
          OrcaVerify.numberStatistic.increaseReadTuple(0);
        } else {
          OrcaVerify.numberStatistic.increaseReadTuple(operationTrace.getReadTupleList().size());
        }
        break;
    }
  }
}
