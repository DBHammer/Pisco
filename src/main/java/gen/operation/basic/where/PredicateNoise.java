package gen.operation.basic.where;

import gen.data.type.DataType;
import gen.operation.enums.PredicateOperator;
import gen.schema.table.Attribute;
import util.xml.Seed;
import util.xml.SeedUtils;

public class PredicateNoise {

  private static final String[] frontNoise = {"-1.0*-1", "(0/-1+1)", "-1e1*-1e-1"};
  private static final String[] backNoise = {
    "\"null\"", "-100000+100000", "(0/-1+101/100-1.01)", "\"aaaa\"", "(1e-14-1e-14)"
  };

  public static String generateNoiseRightValue(String rightValue, Attribute leftValue) {
    DataType dataType = leftValue.getAttrType();

    switch (dataType) {
      case INTEGER:
      case DOUBLE:
      case DECIMAL:
        return numberNoise(rightValue, leftValue);
      default:
        return rightValue;
    }
  }

  /**
   * 生成带有噪声的子查询
   *
   * @param rightValue 要添加噪声的字符串
   * @param leftValue 原始属性名称
   * @param tableName 表名
   * @param predicateOperator 谓词运算符
   * @return 根据属性名称添加噪声的字符串
   */
  public static String generateNoiseSubQuery(
      String rightValue,
      Attribute leftValue,
      String tableName,
      PredicateOperator predicateOperator) {
    return subqueryNoise(rightValue, leftValue.getAttrName(), tableName, predicateOperator);
  }

  /**
   * 默认的噪声生成方法
   *
   * @param rightValue 要添加噪声的字符串
   * @return 根据属性名称添加噪声的字符串
   */
  private static String defaultNoise(String rightValue) {
    return String.format("( select %s)", rightValue);
  }

  // 生成一个等价子查询来满足要求

  /**
   * 将简单的查询谓词转化为一个等价的子查询
   *
   * @param rightValue 谓词的右值
   * @param attrName 列名
   * @param tableName 表名
   * @param predicateOperator
   * @return 等价的子查询
   */
  private static String subqueryNoise(
      String rightValue, String attrName, String tableName, PredicateOperator predicateOperator) {
    // 根据传入的参数predicateOperator，返回不同的查询语句
    switch (predicateOperator) {
      case EQUAL_TO:
      case NOT_EQUAL:
        // 返回查询语句
        return defaultNoise(
            rightValue); // String.format("( select %s_.%s from %s as %s_ where %s_.%s = %s )",
        // tableName, attrName, tableName, tableName, tableName, attrName,
        // rightValue);
      case LESS_EQUAL:
        // 返回查询语句
        return String.format(
            "( select max(%s) from %s as %s_ where %s_.%s %s %s )",
            attrName,
            tableName,
            tableName,
            tableName,
            attrName,
            predicateOperator.getOperator(),
            rightValue);
      case GREATER_EQUAL:
        // 返回查询语句
        return String.format(
            "( select min(%s) from %s as %s_ where %s_.%s %s %s )",
            attrName,
            tableName,
            tableName,
            tableName,
            attrName,
            predicateOperator.getOperator(),
            rightValue);
      case GREATER_THAN:
      case LESS_THAN:
      case ISNULL:
      case IN:
      case LIKE:
      case BETWEEN:
      default:
        // 返回默认的查询语句
        return defaultNoise(rightValue);
    }
  }

  /**
   * 生成带算术表达式噪声的右值 此方法接受两个字符串作为输入：rightValue和leftValue。第一个字符串表示要添加噪声的右值，第二个字符串表示列名。
   * 用于根据属性名称生成带有噪声的右值。
   *
   * @param rightValue 要添加噪声的右值
   * @param leftValue 列名
   * @return 根据属性名称添加噪声的字符串
   */
  private static String numberNoise(String rightValue, Attribute leftValue) {
    String attrName = leftValue.getAttrName();
    return numberFrontNoise() + rightValue + numberBackNoise(attrName);
  }

  private static String numberFrontNoise() {
    Seed frontNoiseSeed = SeedUtils.initSeed(0, frontNoise.length);
    return frontNoise[frontNoiseSeed.getRandomValue()] + "*";
  }

  private static String numberBackNoise(String attrName) {
    Seed backNoiseSeed = SeedUtils.initSeed(0, backNoise.length + 1);
    int idx = backNoiseSeed.getRandomValue();
    if (idx >= backNoise.length) {
      return String.format("+%s/0.1-10*%s", attrName, attrName);
    }

    return "+" + backNoise[idx];
  }
}
