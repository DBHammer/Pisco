package gen.data.param.func;

import gen.data.param.AttrFunc;
import java.io.Serializable;
import java.util.Random;

/** 一种十分简单的PKFunc，result = f(pkId) = pkId */
public class SimpleAttrFunc implements AttrFunc, Serializable {

  private final int randSeedBase;

  public SimpleAttrFunc(int randSeedBase) {
    super();
    this.randSeedBase = randSeedBase;
  }

  @Override
  public int calc(int pkId) {
    // 将pkId作为种子保证确定性
    Random random = new Random(pkId + randSeedBase);
    // 消耗掉一个，以产生随机结果
    random.nextInt();

    // 默认结果可能为负数，此处限定为正值
    return Math.abs(random.nextInt());
  }
}
