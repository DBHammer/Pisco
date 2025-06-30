package gen.data.param;

import java.io.Serializable;
import java.util.Random;
import util.access.distribution.SampleDistribution;

public class AttrParam implements Serializable {
  /** 分布参数 */
  private final SampleDistribution distribution;

  private final int randSeedBase;

  public AttrParam(SampleDistribution distribution) {
    this.distribution = distribution;

    // 生成一个随机数作为种子累加值，增加各个实例间的随机性
    this.randSeedBase = Math.abs(new Random().nextInt());
  }

  /**
   * 静态抽样：根据pkId和分布模式确定性地计算attrId
   *
   * @param pkId pkId
   * @return attrId
   */
  public int staticSample(int pkId) {
    // 将pkId作为种子保证确定性
    Random random = new Random(pkId + randSeedBase);
    // 消耗掉一个，以产生随机结果
    random.nextInt();

    // 默认结果可能为负数，此处限定为正值
    return Math.abs(random.nextInt());
  }

  /**
   * 动态抽样：根据分布模式抽样计算attrId
   *
   * @return attrId
   */
  public int dynamicSample() {
    return distribution.getDataInteger();
  }
}
