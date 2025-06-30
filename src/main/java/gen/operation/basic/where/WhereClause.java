package gen.operation.basic.where;

import config.schema.OperationConfig;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.AttributeGroup;
import gen.schema.table.ForeignKey;
import io.SQLizable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import symbol.Symbol;
import util.xml.Seed;

@Getter
public class WhereClause implements SQLizable, Serializable {
  /** 当前where clause所基于的attribute list */
  private final List<Attribute> baseAttributeList;

  private final List<PredicateLock> predicates;
  private List<String> connect;
  /** -- GETTER -- 投影的属性组的信息，即组的类型(e.g. primarykey)，如果是非主键的属性组，会追加'_id' */
  private List<String> attributeGroupInfo;

  public WhereClause(
      AbstractTable fromClause, OperationConfig.WhereClauseConfig whereClauseConfig) {
    this(fromClause, whereClauseConfig, false, 1);
  }

  /**
   * @param fromClause from clause
   * @param equalOnly if only use equality predicate
   * @param containPrimaryKey 0是只有PK,1是都有，2是没有PK
   */
  public WhereClause(
      AbstractTable fromClause,
      OperationConfig.WhereClauseConfig whereClauseConfig,
      boolean equalOnly,
      int containPrimaryKey) {

    Seed attributeSeed;
    this.predicates = new ArrayList<>();

    // 0.随机确定选择条件的类型，如果containPK为0表示选择PK，为1表示不限制，为2表示不选择PK（用于insert）
    int attributeType;
    if (containPrimaryKey == 0) {
      attributeType = 0;
    } else {
      attributeType = whereClauseConfig.getAttrTypeSeed().getRandomValue();
      if (containPrimaryKey == 2 && attributeType == 0) {
        attributeType = 2;
      }
    }

    this.attributeGroupInfo = new ArrayList<>();
    // 1.根据条件获取选择的属性组，为了防止其溢出，调整生成区间；四种情形分别对应取主键，外键，普通属性和不生成
    switch (attributeType) {
      case 0:
        baseAttributeList = new ArrayList<>(fromClause.getPrimaryKey());
        this.attributeGroupInfo.add(Symbol.PK_ATTR_PREFIX);
        break;
      case 1:
        // 如果没有外键，生成随机数时区间为0，会报错，此时直接转为不生成
        if (fromClause.getCommonForeignKeyList().isEmpty()) {
          baseAttributeList = new ArrayList<>();
          return;
        }
        attributeSeed = whereClauseConfig.getAttrGroupSeed();
        attributeSeed.setRange(0, fromClause.getCommonForeignKeyList().size());

        ForeignKey foreignKey =
            fromClause.getCommonForeignKeyList().get(attributeSeed.getRandomValue());
        baseAttributeList = new ArrayList<>(foreignKey);
        this.attributeGroupInfo.add(Symbol.FK_ATTR_PREFIX + '_' + foreignKey.getId());
        break;
      case 2:
        attributeSeed = whereClauseConfig.getAttrGroupSeed();

        boolean hasUK = !fromClause.getUniqueKey().isEmpty();
        int attrSize = fromClause.getAttributeGroupList().size();
        if (hasUK) {
          attrSize++;
        }
        attributeSeed.setRange(0, attrSize);
        int index = attributeSeed.getRandomValue();
        AttributeGroup attributeGroup;
        if (hasUK && index == (attrSize - 1)) {
          attributeGroup = fromClause.getUniqueKey();
          this.attributeGroupInfo.add(Symbol.UK_ATTR_PREFIX + '_' + attributeGroup.getId());
        } else {
          attributeGroup = fromClause.getAttributeGroupList().get(index);
          this.attributeGroupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' + attributeGroup.getId());
        }
        // AttributeGroup attributeGroup =
        // fromClause.getAttributeGroupList().get(attributeSeed.getRandomValue());
        baseAttributeList = new ArrayList<>(attributeGroup);
        // this.attributeGroupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' + attributeGroup.getId());
        break;
      default:
        baseAttributeList = new ArrayList<>();
        return;
    }
    // 2.确定生成选择条件的配置
    OperationConfig.WhereClauseConfig.PredicateConfig predicateConfig =
        whereClauseConfig.getPredicate();

    int transactType = 0;
    Seed inNumSeed = predicateConfig.getInNumber();
    Seed operationSeed = predicateConfig.getOperatorSeed();
    Map<Integer, Integer> transactMap = new HashMap<>(),
        stroeMap = operationSeed.getDistributionMap();
    // 2.1如果不是只能点查询，就选择查询的方式
    if (!equalOnly) {

      // 2.2选择访问的方式
      transactType = predicateConfig.getTransactTypeSeed().getRandomValue();
      switch (transactType) {
        case 0:
          // 只能是等于
          transactMap.put(0, 100);
          break;
        case 1:
          // 只能是范围
          transactMap.put(1, 25);
          transactMap.put(2, 25);
          transactMap.put(3, 25);
          transactMap.put(4, 25);
          break;
        case 2:
        default:
          // 完全随机
          transactMap = predicateConfig.getOperatorSeed().getDistributionMap();
          inNumSeed = predicateConfig.getInNumber();
          break;
      }
    } else {
      transactMap.put(0, 100);
    }
    operationSeed.setDistributionMap(transactMap);

    // 2.3生成连接符
    this.connect = new ArrayList<>();
    // 如果是完全随机，连接符也是随机的
    if (transactType == 2) {
      Seed connectSeed = whereClauseConfig.getConnectSeed();
      //            connectSeed.setRange(0, 2);
      for (int i = 0; i < baseAttributeList.size() - 1; ++i) {
        connect.add((connectSeed.getRandomValue() == 0 ? "and" : "or"));
      }
    } else {
      // 否则一律用and连接
      for (int i = 0; i < baseAttributeList.size() - 1; ++i) {
        connect.add("and");
      }
    }

    // 2.4生成选择条件
    boolean ifNot = predicateConfig.getIfNotSeed().getRandomValue() == 1;
    // 如果只能是点写、点读，不添加not条件
    if (equalOnly) {
      ifNot = false;
    }
    for (Attribute attribute : baseAttributeList) {
      this.predicates.add(
          new PredicateLock(attribute, operationSeed, fromClause.getTableName(), ifNot, inNumSeed));
    }
    operationSeed.setDistributionMap(stroeMap);
  }

  /**
   * 根据属性组生成点写的where
   *
   * @param baseAttributeList 属性组
   * @param attributeGroupInfo information about attribute group
   */
  public WhereClause(List<Attribute> baseAttributeList, List<String> attributeGroupInfo) {
    // 1.获取属性
    this.baseAttributeList = baseAttributeList;
    this.predicates = new ArrayList<>();
    this.connect = new ArrayList<>();
    this.attributeGroupInfo = attributeGroupInfo;
    // 2.生成predicate和连接符
    for (Attribute attribute : baseAttributeList) {
      this.predicates.add(new PredicateLock(attribute));
      this.connect.add("and");
    }
    if (!this.predicates.isEmpty()) {
      // 删除末尾多余的一个连接符
      this.connect.remove(0);
    }
  }

  /** 一个空的构造函数 */
  public WhereClause() {
    this.baseAttributeList = new ArrayList<>();
    this.predicates = new ArrayList<>();
    this.connect = new ArrayList<>();
  }

  /**
   * 返回where子句
   *
   * @return where clause in string format
   */
  public String toString() {
    return toSQL();
  }

  @Override
  public String toSQL() {
    if (this.predicates.isEmpty()) {
      return "";
    }

    List<String> wordList = new ArrayList<>();
    int i = 0;
    for (; i < this.connect.size(); ++i) {
      PredicateLock predicate = this.predicates.get(i);
      wordList.add(predicate.toString());
      wordList.add((this.connect.get(i)));
    }
    wordList.add(predicates.get(i).toString());
    return String.format("where %s", String.join(" ", wordList));
  }
}
