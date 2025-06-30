package util.xml;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import util.access.distribution.NormalSampleDistribution;
import util.access.distribution.SampleDistribution;
import util.access.distribution.UniformSampleDistribution;
import util.access.distribution.ZipfSampleDistribution;
import util.rand.RandUtils;

/**
 * 封装seed的相关方法和数据
 *
 * @author like_
 */
@Data
public class Seed implements Serializable {

  private Integer begin = 1;
  private Integer end = 2;
  // 分布类型
  @Setter @Getter private String distribution = UNIFORM_DISTRIBUTION;
  // （可能需要的）期望
  @Getter private Double[] parameter;

  // 一个分布器
  private SampleDistribution sampleDistribution;

  @Getter @Setter private Map<Integer, Integer> distributionMap = null;

  public Seed() {}

  public boolean isAvailable() {
    return distribution != null || distributionMap != null;
  }

  // 根据映射表确定概率分布
  public Seed(Map<Integer, Integer> map) {
    distributionMap = map;
  }

  public Seed(Integer begin, Integer end, String distributionDescribe) {
    super();
    this.begin = begin;
    this.end = end;

    String[] distributionDescribeArray = (distributionDescribe.split(","));
    String distribution = distributionDescribeArray[0];
    Double[] parameter = new Double[distributionDescribeArray.length - 1];
    for (int i = 1; i < distributionDescribeArray.length; ++i) {
      parameter[i - 1] = Double.parseDouble(distributionDescribeArray[i]);
    }

    this.distribution = distribution;
    this.parameter = parameter;
    if (this.begin != null && this.end != null) {
      this.updateDistribution();
    }
  }

  /** 获取begin和end之间的一个随机值,采取左闭右开的方法 */
  public int getRandomValue() {
    // 有指定百分比的分布则优先使用
    if (distributionMap != null) {
      return RandUtils.randSelectByProbability(distributionMap);
    }

    // 在已有的生成器代码中已经保证了以下内容，为了保证以后添加的生成器也能保证生成的范围，暂时将这个代码注释保留
    if (this.sampleDistribution == null) {
      updateDistribution();
    }
    int randomValue = this.sampleDistribution.getDataInteger();
    while (randomValue >= this.end || randomValue < this.begin) {
      randomValue = this.sampleDistribution.getDataInteger();
    }

    return randomValue;
  }

  public void setRange(Integer begin, Integer end) {
    this.begin = begin;
    this.end = end;
    if (begin != null && end != null) {
      this.updateDistribution();
    }
  }

  private void updateDistribution() {
    // 选择使用的分布参数
    switch (this.distribution.toUpperCase()) {
      case UNIFORM_DISTRIBUTION:
        sampleDistribution = new UniformSampleDistribution(begin, end);
        break;
      case NORMAL_DISTRIBUTION:
        sampleDistribution = new NormalSampleDistribution(begin, end);
        break;
      case ZIPF_DISTRIBUTION:
        // Zipf分布需要exponent参数，若不提供则抛出异常
        if (parameter.length == 0) {
          throw new RuntimeException("Parameter missed:exponent is needed for Zipf distribution!");
        }
        sampleDistribution = new ZipfSampleDistribution(begin, end, parameter[0]);
        break;
      default:
        // 若分布未知则抛出异常
        throw new RuntimeException(
            "Parameter error:The distribution '"
                + this.distribution
                + "' does not exist in the lib!");
    }
  }

  @Override
  public String toString() {
    return "begin "
        + this.begin
        + "\n"
        + "end "
        + this.end
        + "\n"
        + "distribution "
        + this.distribution
        + "\n"
        + "parameter "
        + Arrays.toString(this.parameter)
        + "\n";
  }

  public static final String NORMAL_DISTRIBUTION = "NORMAL";
  public static final String UNIFORM_DISTRIBUTION = "UNIFORM";
  public static final String ZIPF_DISTRIBUTION = "ZIPF";
}
