package gen.data.generator;

import static dbloader.transaction.TransactionLoader.logger;

import gen.data.param.AttrFunc;
import gen.data.param.AttrParam;
import gen.data.param.DataRepo;
import gen.data.type.DataType;
import gen.data.value.AttrValue;

/** 利用生成参数进行UK生成的接口 */
public class UKGenerator {

  public static int staticUKId(int pkId, AttrParam attrParam) {
    return attrParam.staticSample(pkId);
  }

  public static int dynamicAttrId(AttrParam attrParam) {
    return attrParam.dynamicSample();
  }

  public static AttrValue ukId2Attr(int ukId, DataType type, AttrFunc attrFunc, DataRepo repo) {
    Object value;
    switch (type) {
      case INTEGER:
        value = repo.getIntegerList().get(attrFunc.calc(ukId) % repo.getIntegerList().size());
        break;
      case VARCHAR:
        value = repo.getVarcharList().get(attrFunc.calc(ukId) % repo.getVarcharList().size());
        break;
      case DOUBLE:
        value = repo.getDoubleList().get(attrFunc.calc(ukId) % repo.getDoubleList().size());
        break;
      case DECIMAL:
        value = repo.getDecimalList().get(attrFunc.calc(ukId) % repo.getDecimalList().size());
        break;
      case TIMESTAMP:
        value = repo.getTimestampList().get(attrFunc.calc(ukId) % repo.getTimestampList().size());
        break;
      case BLOB:
        value = repo.getBlobList().get(attrFunc.calc(ukId) % repo.getBlobList().size());
        break;
      case BOOL:
        value = repo.getBoolList().get(attrFunc.calc(ukId) % repo.getBoolList().size());
        break;
      default:
        throw new RuntimeException("不支持的类型: " + type.name());
    }

    return new AttrValue(type, value);
  }

  public static int uk2index(AttrValue attrValue, DataRepo repo) {
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

  public static AttrValue index2attr(int index, DataType type, DataRepo repo) {
    // 现在的实现是直接在case语句中return，所以没有break
    Object value = null;
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
        logger.warn("不支持的数据类型");
    }
    return new AttrValue(type, value);
  }
}
