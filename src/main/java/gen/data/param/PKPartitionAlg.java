package gen.data.param;

import gen.shadow.PartitionTag;

public interface PKPartitionAlg {
  /**
   * 根据pkId计算分区
   *
   * @param pkId pkId
   * @return 静态、动态存在、动态不存在
   */
  PartitionTag partition(int pkId);
}
