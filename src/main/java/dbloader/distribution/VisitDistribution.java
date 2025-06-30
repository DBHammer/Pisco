package dbloader.distribution;

import config.schema.DistributionType;
import org.apache.commons.math3.distribution.*;

public class VisitDistribution {

  private final DistributionType distributionType;
  private AbstractRealDistribution realDistribution;
  private AbstractIntegerDistribution integerDistribution;

  public VisitDistribution(DistributionType type, String params) {
    super();
    this.distributionType = type;
    if (type == DistributionType.NORMAL) {
      realDistribution = getNormalDistribution(params);
    } else if (type == DistributionType.UNIFORM) {
      integerDistribution = getUniformIntegerDistribution(params);
    } else if (type == DistributionType.ZIPF) {
      integerDistribution = getZipfDistribution(params);
    }
  }

  public int sample() {
    if (distributionType == DistributionType.NORMAL) {
      return (int) realDistribution.sample();
    } else {
      return integerDistribution.sample();
    }
  }

  public NormalDistribution getNormalDistribution(String params) {
    double mu = Double.parseDouble(params.split(",")[0].trim());
    double sigma = Double.parseDouble(params.split(",")[1].trim());
    return new NormalDistribution(mu, sigma);
  }

  public UniformIntegerDistribution getUniformIntegerDistribution(String params) {
    int begin = Integer.parseInt(params.split(",")[0].trim());
    int end = Integer.parseInt(params.split(",")[1].trim());
    return new UniformIntegerDistribution(begin, end);
  }

  public ZipfDistribution getZipfDistribution(String params) {
    int r = Integer.parseInt(params.split(",")[0].trim());
    double alpha = Double.parseDouble(params.split(",")[1].trim());
    return new ZipfDistribution(r, alpha);
  }
}
