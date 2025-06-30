package gen.data.generator;

import gen.data.param.FKParam;
import gen.data.param.PKParam;
import gen.data.type.DataType;
import gen.data.value.AttrValue;

/** 利用生成参数进行FK生成的接口 */
public class FKGenerator {
  /**
   * 确定性计算fkId(本质上是另一张表pkId)
   *
   * @param pkId pkId
   * @param fkParam fkParam
   * @return fkId(本质上是另一张表pkId)
   */
  public static int staticFkId(int pkId, FKParam fkParam) {
    return fkParam.staticSample(pkId);
  }

  /**
   * 非确定性抽样fkId(本质上是另一张表pkId)
   *
   * @param fkParam fkParam
   * @return fkId(本质上是另一张表pkId)
   */
  public static int dynamicFkId(FKParam fkParam) {
    return fkParam.dynamicSample();
  }

  /**
   * 把fkId转换成具体外键值（具体外键值的含义以及表示有待进一步明确）
   *
   * @param fkId fkId
   * @return 具体外键值
   */
  public static AttrValue fkId2Fk(int fkId, DataType type, PKParam pkParam) {
    return PKGenerator.pkId2Pk(fkId, type, pkParam);
  }

  /**
   * 把具体外键值转换成fkId（具体外键值的含义以及表示有待进一步明确）
   *
   * @param fkValue 具体外键值
   * @return fkId
   */
  public static int fk2FkId(AttrValue fkValue, PKParam pkParam) {
    return PKGenerator.pk2PkId(fkValue, pkParam);
  }
}
