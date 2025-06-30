package util.access.distribution;

import java.io.Serializable;
import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 * zipf分布 针对一组离散值组成的集合，集合里每个元素出现的频率为F,按照频率F给每个元素进行排序，频率高的排在前面，这样每个元素有一个排名R 频率F与排名R成反比 ，即F=C/R,C是一个常数
 *
 * @author like_
 */
public class ZipfSampleDistribution implements SampleDistribution, Serializable {

  private static final long serialVersionUID = 1L;

  /** 利用apache.commons.math现成的工具实现zipf分布 */
  private final ZipfDistribution zipfGenerator;

  private final int begin;

  private final int end;

  public ZipfSampleDistribution(int begin, int end, double skew) {
    super();
    this.begin = begin;
    this.end = end;
    /*
     zipf分布的倾斜程度
    */
    zipfGenerator = new ZipfDistribution(this.end - this.begin, skew);
  }

  /** 按照zipf在指定区间内获取一个随机数，可以是小数，也可以是整数 */
  @Override
  public double getData() {
    return getDataInteger(); // 这里暂时省去末尾的小数 TODO
  }

  /** 按照zipf在指定区间内获取一个随机整数 */
  @Override
  public int getDataInteger() {
    return zipfGenerator.sample() + this.begin - 1;
  }
}
