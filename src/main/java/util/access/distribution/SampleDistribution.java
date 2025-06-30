package util.access.distribution;

import config.schema.DistributionType;

/**
 * 访问分布的统一接口，一个按照分布getdata的方法
 *
 * @author like_
 */
public interface SampleDistribution {
  /**
   * 按照分布在指定区间[a,b)获取一个数，可以是小数，也可以是整数
   *
   * @return data
   */
  double getData();

  /**
   * 按照分布在指定区间[a,b)获取一个整数
   *
   * @return int
   */
  int getDataInteger();

  static SampleDistribution getAccessDistribution(
      DistributionType type, int begin, int end, Double[] parameter) {

    if (begin >= end) throw new RuntimeException("begin must be less than end, begin < end!");

    switch (type) {
      case UNIFORM:
        return new UniformSampleDistribution(begin, end);
      case NORMAL:
        return new NormalSampleDistribution(begin, end);
      case ZIPF:
        // Zipf分布需要exponent参数，若不提供则抛出异常
        if (parameter.length == 0) {
          throw new RuntimeException("Parameter missed:exponent is needed for Zipf distribution!");
        }
        return new ZipfSampleDistribution(begin, end, parameter[0]);
      default:
        // 若分布未知则抛出异常
        throw new RuntimeException(
            "Parameter error:The distribution '" + type + "' does not exist in the lib!");
    }
  }
}
