package util.access.distribution;

import java.io.Serializable;
import org.apache.commons.math3.distribution.NormalDistribution;

public class NormalSampleDistribution implements SampleDistribution, Serializable {

  private static final long serialVersionUID = 1L;

  /** 利用apache.commons.math现成的工具实现normal分布 */
  private final NormalDistribution normalGenerator;

  private final int begin;

  private final int end;

  private final double mean;

  private final double standardDeviation;

  /**
   * 通过区间的两个短点begin和end，根据3σ原则，即正态分布的大部分点都落在这个区间内（μ-3σ,μ+3σ），极小概率会在其他区间，因此，一旦区间确定之后，我们可以计算出均值和方差
   *
   * @param begin 起始值
   * @param end 结束值
   */
  public NormalSampleDistribution(int begin, int end) {
    super();

    this.begin = begin;
    this.end = end;
    this.mean = (this.end + this.begin) / 2.0d;
    this.standardDeviation = (this.end - this.begin) / 6.0d;

    normalGenerator = new NormalDistribution(mean, standardDeviation);
  }

  @Override
  public double getData() {
    // 利用数学公式，将标准正太分布，转化成指定方差和均值的正太分布
    double temp = normalGenerator.sample();

    // 因为任然有概率会超出区间的数据，因此我们每次生成一个temp都需要判断是否超出区间，超出就再生成一个
    while (temp < this.begin || temp > this.end) {
      temp = normalGenerator.sample();
    }

    return temp;
  }

  @Override
  public int getDataInteger() {
    return (int) getData();
  }

  @Override
  public String toString() {
    return "NormalAccessDistribution [begin="
        + begin
        + ", end="
        + end
        + ", mean="
        + mean
        + ", standardDeviation="
        + standardDeviation
        + "]";
  }
}
