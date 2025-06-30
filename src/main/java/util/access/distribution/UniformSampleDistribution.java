package util.access.distribution;

import java.io.Serializable;
import java.util.Random;

/**
 * 均匀分布访问器 在一个数值区间内按均匀分布的方式获取一个数据
 *
 * @author like_
 */
public class UniformSampleDistribution implements SampleDistribution, Serializable {

  private static final long serialVersionUID = 1L;

  private final Random random;

  private final int begin;

  private final int end;

  public UniformSampleDistribution(int begin, int end) {
    super();
    random = new Random();
    this.begin = begin;
    this.end = end;
  }

  /** 按照均匀分布获取一个区间内的数，可以是小数，也可以是整数 */
  public double getData() {
    return this.getDataInteger() + random.nextDouble();
  }

  /** 按照均匀分布获取一个区间内的整数 */
  public int getDataInteger() {
    return random.nextInt(this.end - this.begin) + this.begin;
  }
}
