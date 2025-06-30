package gen.data.param.func;

import gen.data.param.PKPartitionAlg;
import gen.shadow.PartitionTag;
import java.io.Serializable;
import java.util.Random;

public class HashPartitionAlg implements PKPartitionAlg, Serializable {

  private final double probOfStatic;
  private final double probOfDynamicExists;
  private final int randSeedBase;

  /**
   * 简单的确定性分区算法
   *
   * @param probOfStatic 静态区出现概率
   * @param probOfDynamicExists 动态区存在初始数据的概率
   * @param randSeedBase 此值累加到pkId上作为seed
   */
  public HashPartitionAlg(double probOfStatic, double probOfDynamicExists, int randSeedBase) {
    super();

    // 两者概率小于等于1，剩下的是动态区不存在的概率
    assert probOfStatic + probOfDynamicExists <= 1;

    this.probOfStatic = probOfStatic;
    this.probOfDynamicExists = probOfDynamicExists;
    this.randSeedBase = randSeedBase;
  }

  @Override
  public PartitionTag partition(int pkId) {

    Random random = new Random(pkId + randSeedBase);

    // 消耗掉一个随机数，因为若种子连续，随机数也会十分接近
    // 会集中在某个范围内，无法达到随机分布的目的
    // 消耗掉一个即可解决这个问题
    random.nextDouble();

    // 以 pkId 作为种子，保证随机数确定性
    double rand = random.nextDouble();

    if (rand < probOfStatic) {
      return PartitionTag.STATIC;
    } else if (rand < probOfStatic + probOfDynamicExists) {
      return PartitionTag.DYNAMIC_EXISTS;
    } else {
      return PartitionTag.DYNAMIC_NOT_EXISTS;
    }
  }
}
