package gen.data.generator;

import config.schema.DistributionType;
import gen.data.param.AttrParam;
import org.junit.Test;
import util.access.distribution.SampleDistribution;

public class AttrGeneratorTest {

  @Test
  public void staticAttrId() {
    AttrParam attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.UNIFORM, 0, 100, null));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.staticAttrId(i, attrParam);
      assert pkId >= 0;
    }

    attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.NORMAL, 0, 100, null));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.staticAttrId(i, attrParam);
      assert pkId >= 0;
    }

    Double[] paras = new Double[] {1.0};
    attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.ZIPF, 0, 100, paras));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.staticAttrId(i, attrParam);
      assert pkId >= 0;
    }
  }

  @Test
  public void dynamicAttrId() {

    AttrParam attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.UNIFORM, -100, 1, null));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.dynamicAttrId(attrParam);
      assert pkId >= -100 && pkId < 1;
    }

    attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.NORMAL, 0, 100, null));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.dynamicAttrId(attrParam);
      assert pkId >= 0 && pkId < 100;
    }

    Double[] paras = new Double[] {1.0};
    attrParam =
        new AttrParam(
            SampleDistribution.getAccessDistribution(DistributionType.ZIPF, -10, 100, paras));
    for (int i = -10; i < 10; i++) {
      int pkId = AttrGenerator.dynamicAttrId(attrParam);
      assert pkId >= -10 && pkId < 100;
    }
  }
}
