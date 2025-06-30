package gen.data.generator;

import gen.data.param.AttrFunc;
import gen.data.param.AttrParam;
import gen.data.param.DataRepo;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** 作为利用生成参数进行数据生成的接口 */
public class AttrGenerator {
  private static final Logger logger = LogManager.getLogger(AttrGenerator.class);

  public static int staticAttrId(int pkId, AttrParam attrParam) {
    return attrParam.staticSample(pkId);
  }

  public static int dynamicAttrId(AttrParam attrParam) {
    return attrParam.dynamicSample();
  }

  /**
   * 将attrId转换成具体attr值
   *
   * @param attrId attrId
   * @param type 类型
   * @param attrFunc attrFunc
   * @param repo ValueListRepo
   * @return tuple的具体值
   */
  public static AttrValue attrId2Attr(int attrId, DataType type, AttrFunc attrFunc, DataRepo repo) {
    Object value;
    switch (type) {
      case INTEGER:
        value = repo.getIntegerList().get(attrFunc.calc(attrId) % repo.getIntegerList().size());
        break;
      case VARCHAR:
        value = repo.getVarcharList().get(attrFunc.calc(attrId) % repo.getVarcharList().size());
        break;
      case DOUBLE:
        value = repo.getDoubleList().get(attrFunc.calc(attrId) % repo.getDoubleList().size());
        break;
      case DECIMAL:
        value = repo.getDecimalList().get(attrFunc.calc(attrId) % repo.getDecimalList().size());
        break;
      case TIMESTAMP:
        value = repo.getTimestampList().get(attrFunc.calc(attrId) % repo.getTimestampList().size());
        break;
      case BLOB:
        value = repo.getBlobList().get(attrFunc.calc(attrId) % repo.getBlobList().size());
        break;
      case BOOL:
        value = repo.getBoolList().get(attrFunc.calc(attrId) % repo.getBoolList().size());
        break;
      default:
        throw new RuntimeException("不支持的类型: " + type.name());
    }

    return new AttrValue(type, value);
  }

  /**
   * 具体属性值转换成其在ValueList中的索引
   *
   * @param attrValue 具体属性值
   * @param repo ValueListRepo
   * @return 具体属性值在valueList里的索引位置
   */
  public static int attr2index(AttrValue attrValue, DataRepo repo) {
    // 现在的实现是直接在case语句中return，所以没有break
    switch (attrValue.type) {
      case INTEGER:
        return repo.getIntegerList().indexOf(attrValue.value);
      case VARCHAR:
        return repo.getVarcharList().indexOf(attrValue.value);
      case DOUBLE:
        return repo.getDoubleList().indexOf(attrValue.value);
      case DECIMAL:
        return repo.getDecimalList().indexOf(attrValue.value);
      case TIMESTAMP:
        return repo.getTimestampList().indexOf(attrValue.value);
      case BLOB:
        return repo.getBlobList().indexOf(attrValue.value);
      case BOOL:
        return repo.getBoolList().indexOf(attrValue.value);
      default:
        throw new RuntimeException("不支持的类型: " + attrValue.type.name());
    }
  }

  /**
   * 具体属性值转换成其在ValueList中的索引
   *
   * @param index 索引
   * @param type 类型
   * @param repo valueListRepo
   * @return attrValue
   */
  public static AttrValue index2attr(int index, DataType type, DataRepo repo) {
    // 现在的实现是直接在case语句中return，所以没有break
    Object value = null;
    try {
      switch (type) {
        case INTEGER:
          value = repo.getIntegerList().get(index);
          break;
        case VARCHAR:
          value = repo.getVarcharList().get(index);
          break;
        case DOUBLE:
          value = repo.getDoubleList().get(index);
          break;
        case DECIMAL:
          value = repo.getDecimalList().get(index);
          break;
        case TIMESTAMP:
          value = repo.getTimestampList().get(index);
          break;
        case BLOB:
          value = repo.getBlobList().get(index);
          break;
        case BOOL:
          value = repo.getBoolList().get(index);
          break;
        default:
          throw new RuntimeException("不支持的类型: " + type.name());
      }
    } catch (Exception e) {
      logger.warn(e);
    }
    return new AttrValue(type, value);
  }
}
