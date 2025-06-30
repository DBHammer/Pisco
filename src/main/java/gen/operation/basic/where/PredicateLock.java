package gen.operation.basic.where;

import context.OrcaContext;
import gen.data.type.DataType;
import gen.operation.enums.PredicateOperator;
import gen.schema.table.Attribute;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import symbol.Symbol;
import util.xml.Seed;
import util.xml.SeedUtils;

public class PredicateLock implements Serializable {
  @Getter private final List<String> rightValue;
  @Getter private final Attribute leftValue;
  @Getter private PredicateOperator predicateOperator;
  private final String transactString;
  private boolean isNot;
  @Getter private int parameterNumber;

  private String sql = null;

  private String tableName;

  public PredicateLock(
      Attribute leftValue, Seed operatorTypeSeed, String tableName, boolean ifNot, Seed inNumSeed) {
    // 0.获取左值，即对应的属性，并得到属性的类型
    this.leftValue = leftValue;
    DataType type = leftValue.getAttrType();
    this.rightValue = new ArrayList<>();
    // 1.先判断要不要加上not
    this.isNot = ifNot;
    this.tableName = tableName;

    // 2.再给出判断条件的类型
    this.predicateOperator = PredicateOperator.values()[operatorTypeSeed.getRandomValue()];
    // 2.1特判blob，如果是blob条件改为is null
    if (type == DataType.BLOB) {
      this.predicateOperator = PredicateOperator.ISNULL;
    }

    // 2.2调整判断条件的类型，以保证和属性间是合法的对应关系
    int i = 0;
    while (i < MAX_CHANGE_TIME) {
      if (this.judgeTransactType(type) && this.judgeAttributeType(leftValue)) {
        break;
      }
      this.predicateOperator = PredicateOperator.values()[operatorTypeSeed.getRandomValue()];
      i++;
    }
    // 如果一直取不到，就转成=
    if (i == MAX_CHANGE_TIME) {
      this.predicateOperator = PredicateOperator.EQUAL_TO;
    }

    // 2.3判断条件生成对应的字符串
    this.transactString = this.generateTransactString(inNumSeed);

    // 3.生成右值

    this.rightValue.add("?");
  }

  /**
   * 只用于生成相等的关系
   *
   * @param leftValue left value
   */
  public PredicateLock(Attribute leftValue) {
    this.leftValue = leftValue;
    this.rightValue = new ArrayList<>();
    this.isNot = false;
    this.rightValue.add("?");
    this.parameterNumber = 1;
    this.predicateOperator = PredicateOperator.EQUAL_TO;
    this.transactString = "=";
  }

  /**
   * 判断属性类型和判断类型是否匹配
   *
   * @param type 属性类型
   * @return if type is correct
   */
  private boolean judgeTransactType(DataType type) {
    // blob只能是is null
    if (type == DataType.BLOB && this.predicateOperator != PredicateOperator.ISNULL) {
      return false;
    }
    // 只有varchar 支持like
    return type == DataType.VARCHAR || this.predicateOperator != PredicateOperator.LIKE;
  }

  /**
   * 检查在isnull情况下是否是主键或外键，这两种情况是无意义的
   *
   * @param attribute attribute
   * @return false if isnull is used for pk or fk
   */
  private boolean judgeAttributeType(Attribute attribute) {
    PredicateOperator transact = this.predicateOperator;
    if (transact == PredicateOperator.ISNULL) {
      return !attribute.getAttrName().contains(Symbol.PK_ATTR_PREFIX)
          && !attribute.getAttrName().contains(Symbol.FK_ATTR_PREFIX);
    }
    return true;
  }

  private String generateTransactString(Seed inNumSeed) {
    this.parameterNumber = 1;
    switch (this.predicateOperator) {
      case EQUAL_TO:
      case GREATER_THAN:
      case LESS_THAN:
      case GREATER_EQUAL:
      case LESS_EQUAL:
      case LIKE:
        break;
      case NOT_EQUAL:
        this.isNot = !this.isNot;
        break;
      case IN:
        this.parameterNumber = inNumSeed.getRandomValue();
        for (int i = 0; i < this.parameterNumber - 1; i++) {
          this.rightValue.add("?");
        }
        break;
      case BETWEEN:
        // 需要特判
        this.rightValue.add("?");
        this.parameterNumber = 2;
        break;
      case ISNULL:
      default:
        this.rightValue.clear();
        this.parameterNumber = 0;
        return "is null";
    }

    return this.predicateOperator.getOperator();
  }

  public boolean isNot() {
    return isNot;
  }

  /**
   * 获得单个判断条件对应的字符串,即where后面的部分，用()包裹
   *
   * @return string
   */
  public String toString() {
    if (this.sql != null) {
      return this.sql;
    }

    // 在最开头加上not
    List<String> token = new ArrayList<>();
    token.add("(");

    // 先在最前面加上not
    if (isNot) {
      token.add("not");
    }

    // 左值
    token.add(String.format("`%s`", this.leftValue.getAttrName()));

    // 运算符
    token.add(this.transactString);

    if (this.predicateOperator == PredicateOperator.ISNULL) {
      // 如果是is null，不需要右值
    } else if (this.predicateOperator == PredicateOperator.BETWEEN) {
      // between and需要两个
      token.add(noiseRightValue(this.rightValue.get(0)));
      token.add("and");
      token.add(noiseRightValue(this.rightValue.get(1)));
    } else if (this.predicateOperator == PredicateOperator.IN) {
      // in
      token.add("(");
      token.add(String.join(" , ", this.rightValue));
      token.add(")");
    } else {
      token.add(noiseRightValue(this.rightValue.get(0)));
    }
    token.add(" )");

    this.sql = String.join(" ", token);
    return this.sql;
  }

  /**
   * 通过一个真实值列表转字符串
   *
   * @param rightValues 真实的右值
   * @return 填充了真实右值的字符串
   */
  public String toString(String leftValue, List<String> rightValues) {
    // 在最开头加上not
    StringBuilder temp =
        new StringBuilder(
            " " + (isNot ? "not " : "") + leftValue + " " + this.transactString + " ");
    if (this.predicateOperator == PredicateOperator.BETWEEN) {
      // between and需要两个
      temp.append(rightValues.get(0)).append(" and ").append(rightValues.get(1));
    } else if (this.predicateOperator == PredicateOperator.IN) {
      // in
      temp.append("(").append(String.join(", ", rightValues)).append(")");
    } else {
      temp.append(rightValues.get(0));
    }
    temp.append(" ");
    return String.valueOf(temp);
  }

  private String noiseRightValue(String rightValue) {

    Seed noiseSeed = OrcaContext.configColl.getTransaction().getIfNoise();
    if (noiseSeed.getRandomValue() == 1) {
      Seed noiseTypeSeed = SeedUtils.initSeed(0, 2);
      int noiseType = noiseTypeSeed.getRandomValue();
      // 1 表示fuzzing为子查询 否则是fuzzing为表达式
      if (noiseType == 1) {
        // 如果不是主键，子查询可能返回多个值，所以要把过滤谓词改为in
        //                predicateOperator = PredicateOperator.IN;
        //                transactString = PredicateOperator.IN.getOperator();

        return PredicateNoise.generateNoiseSubQuery(
            rightValue, leftValue, tableName, predicateOperator);
      } else {
        return PredicateNoise.generateNoiseRightValue(rightValue, leftValue);
      }
    }

    return rightValue;
  }

  private static final int MAX_CHANGE_TIME = 10;

  public boolean getIsNot() {
    return this.isNot;
  }
}
