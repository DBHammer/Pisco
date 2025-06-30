package gen.data.param.func;

import gen.shadow.PartitionTag;
import org.junit.Test;

public class HashPartitionAlgTest {

  @Test
  public void partition() {
    HashPartitionAlg hashPartitionAlg = new HashPartitionAlg(0.70, 0.15, 1);
    int staticCount = 0;
    int dynamicCount = 0;

    for (int i = 0; i < 1000; i++) {

      PartitionTag partitionTag = hashPartitionAlg.partition(i);

      if (partitionTag == PartitionTag.STATIC) {
        staticCount++;
      } else {
        dynamicCount++;
      }

      System.out.printf("%d - %s\n", i, partitionTag.name());
    }

    System.out.printf("[staticCount = %d]\n", staticCount);
    System.out.printf("[dynamicCount = %d]\n", dynamicCount);
  }
}
