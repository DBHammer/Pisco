package gen.data.generator;

import gen.data.param.PKParam;
import gen.data.param.PKPartitionAlg;
import gen.data.param.func.HashPartitionAlg;
import gen.data.param.func.SimplePKFunc;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import org.junit.Test;

public class PKGeneratorTest {

  @Test
  public void test() {
    PKParam pkParam = new PKParam(11, new SimplePKFunc(1));
    PKPartitionAlg partitionAlg = new HashPartitionAlg(0.70, 0.15, 1);

    for (int pkId = 0; pkId < 100; pkId++) {

      AttrValue pkValue = PKGenerator.pkId2Pk(pkId, DataType.VARCHAR, pkParam);

      System.out.printf(
          "%d\t%s\t%s\t%d\n",
          pkId,
          PKGenerator.calcPartition(pkId, partitionAlg),
          pkValue.value,
          PKGenerator.pk2PkId(pkValue, pkParam));
    }
  }
}
