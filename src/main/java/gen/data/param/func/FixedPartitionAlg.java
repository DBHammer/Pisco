package gen.data.param.func;

import gen.data.param.PKPartitionAlg;
import gen.shadow.PartitionTag;
import java.io.Serializable;

public class FixedPartitionAlg implements PKPartitionAlg, Serializable {

  private final int staticUpper;
  private final int dynamicExistsUpper;

  /**
   * 简单的确定性分区算法
   *
   * @param probOfStatic 静态区出现概率
   * @param probOfDynamicExists 动态区存在初始数据的概率
   * @param tableSize 表大小
   */
  public FixedPartitionAlg(double probOfStatic, double probOfDynamicExists, int tableSize) {
    super();

    // 两者概率小于等于1，剩下的是动态区不存在的概率
    assert probOfStatic + probOfDynamicExists <= 1;

    this.staticUpper = (int) (tableSize * probOfStatic);
    this.dynamicExistsUpper = (int) (tableSize * (probOfStatic + probOfDynamicExists));
  }

  @Override
  public PartitionTag partition(int pkId) {

    if (pkId < staticUpper) {
      return PartitionTag.STATIC;
    } else if (pkId < dynamicExistsUpper) {
      return PartitionTag.DYNAMIC_EXISTS; // 这里导致返回 动态存在
    } else {
      return PartitionTag.DYNAMIC_NOT_EXISTS;
    }
  }
}
