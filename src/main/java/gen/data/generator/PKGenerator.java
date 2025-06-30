package gen.data.generator;

import gen.data.param.PKParam;
import gen.data.param.PKPartitionAlg;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.schema.generic.AbstractTable;
import gen.shadow.DBMirror;
import gen.shadow.PartitionTag;
import gen.shadow.TableMirror;
import java.util.HashMap;

/** 利用生成参数进行PK生成的接口 */
public class PKGenerator {
  /**
   * 计算pkId所属分区
   *
   * @param pkId pkId
   * @param pkPartitionAlg pkPartitionAlg
   * @return Tag
   */
  public static PartitionTag calcPartition(int pkId, PKPartitionAlg pkPartitionAlg) {
    // 具体算法还不确定
    return pkPartitionAlg.partition(pkId);
  }

  private static final HashMap<Integer, Integer> bias = new HashMap<>();

  /**
   * 更新将pkId转换成新的具体主键值时的偏移量
   *
   * @param pkId pkId
   */
  public static synchronized void pkId2newPk(int pkId) {
    // 每次递增1
    bias.put(pkId, 1 + bias.getOrDefault(pkId, 0));
  }

  /**
   * 将pkId转换成具体主键值
   *
   * @param pkId pkId
   * @param type 类型
   * @param pkParam pkParameter
   * @return 具体主键值
   */
  public static AttrValue pkId2Pk(int pkId, DataType type, PKParam pkParam) {

    // 此主键的区间起始值，即A_i
    int pkIdBase = pkId * pkParam.step;

    // 随机数
    // 注意，此处要求值为正，否则主键可能重复
    int rand = Math.abs((pkParam.pkFunc.calc(pkId) + bias.getOrDefault(pkId, 0)) % pkParam.step);

    // int主键值
    int pkInt = rand + pkIdBase;

    switch (type) {
      case INTEGER:
        return new AttrValue(type, pkInt);
      case VARCHAR:
        return new AttrValue(type, "vc" + pkInt);
      case DOUBLE:
      case DECIMAL:
        return new AttrValue(type, (double) pkInt);
      default:
        throw new RuntimeException("当前主键类型不支持: " + type.name());
    }
  }

  /**
   * 将具体主键值转换成pkId
   *
   * @param pkValue 具体主键值
   * @param pkParam pk生成参数，用于反推pkId
   * @return pkId
   */
  public static int pk2PkId(AttrValue pkValue, PKParam pkParam) {
    int pkInt;

    switch (pkValue.type) {
      case INTEGER:
      case DECIMAL:
      case DOUBLE:
        pkInt = (int) pkValue.value;
        break;
      case VARCHAR:
        pkInt = Integer.parseInt(((String) pkValue.value).substring(2));
        break;
      default:
        throw new RuntimeException("当前主键类型不支持: " + pkValue.type.name());
    }

    return pkInt / pkParam.step;
  }

  /**
   * 向下搜寻一个属于静态区的pkId
   *
   * @return static pkId
   */
  public static int downForStaticPkId(DBMirror dbMirror, AbstractTable table, int pkId) {
    TableMirror tableMirror = dbMirror.getTableMirrorById(table.getTableId());
    // search for a static fkId
    while (PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg())
        != PartitionTag.STATIC) {
      pkId = (pkId + 1) % tableMirror.getMaxSize();
    }
    return pkId;
  }

  /**
   * 向下搜寻一个属于静态区或者属于动态区且初始标签为EXISTS的pkId
   *
   * @return static pkId or dynamic_exists pkId
   */
  public static int downForStaticOrDynamicExistsPkId(
      DBMirror dbMirror, AbstractTable table, int pkId) {
    TableMirror tableMirror = dbMirror.getTableMirrorById(table.getTableId());
    // search for a static or dynamic_exists fkId
    while (PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg())
        == PartitionTag.DYNAMIC_NOT_EXISTS) {
      pkId = (pkId + 1) % tableMirror.getMaxSize();
    }
    return pkId;
  }
}
