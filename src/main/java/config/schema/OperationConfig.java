package config.schema;

import gen.operation.enums.*;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import util.xml.Seed;
import util.xml.SeedUtils;

@Data
public class OperationConfig {

  @Getter private boolean updatePK = false;

  //    @Getter
  //    private boolean noWhere = false;
  private Seed updateRangeSeed = null;
  @Getter private Map<String, Object> updateRange = null;

  public Seed getUpdateRangeSeed() {
    if (updateRangeSeed == null) {
      updateRangeSeed = SeedUtils.initSeed(updateRange, UpdateType.class);
    }
    return updateRangeSeed;
  }

  @Data
  public static class FromClauseConfig {
    private Seed fromClauseSeed;
    private Seed typeSeed;

    public FromClauseConfig() {
      fromClauseSeed = SeedUtils.initSeed(0, 1);
      typeSeed = SeedUtils.initSeed(0, 1);
    }
  }

  @Data
  public static class ProjectConfig {
    private Seed attrGroupSeed = new Seed();
    private Seed attrTypeSeed = null;
    private Seed attributeNumber = new Seed();
    private Map<String, Object> attributeType = null;

    public Seed getAttrTypeSeed() {
      if (attrTypeSeed == null) {
        attrTypeSeed = SeedUtils.initSeed(attributeType, SelectType.class);
      }
      return attrTypeSeed;
    }

    public ProjectConfig() {}
  }

  @Data
  public static class WhereClauseConfig {
    private Seed attrTypeSeed = null;
    private Map<String, Object> attributeType;
    private Seed attrGroupSeed = new Seed();
    private Seed connectSeed = null;

    @Data
    public static class PredicateConfig {
      private Seed inNumber = null;
      private Seed transactTypeSeed = null;
      private Seed operatorSeed = null;
      private Map<String, Object> operator;
      private Seed ifNotSeed = null;

      public Seed getOperatorSeed() {
        if (operatorSeed == null) {
          if (operator == null) {
            operatorSeed = SeedUtils.initSeed(0, 1);
          } else {
            operatorSeed = SeedUtils.initSeed(operator, PredicateOperator.class);
          }
        }
        return operatorSeed;
      }

      public Seed getInNumber() {
        if (inNumber == null) {
          inNumber = SeedUtils.initSeed(1, 2);
        }
        return inNumber;
      }

      public Seed getTransactTypeSeed() {
        if (transactTypeSeed == null) {
          // 强行指定为第三种，也就是完全随机
          transactTypeSeed = SeedUtils.initSeed(2, 3);
        }
        return transactTypeSeed;
      }

      public Seed getIfNotSeed() {
        if (ifNotSeed == null) {
          ifNotSeed = SeedUtils.initSeed(0, 1);
        }
        return ifNotSeed;
      }

      public PredicateConfig() {}
    }

    private PredicateConfig predicate = new PredicateConfig();

    public Seed getAttrTypeSeed() {
      if (attrTypeSeed == null) {
        attrTypeSeed = SeedUtils.initSeed(attributeType, ColumnType.class);
      }
      return attrTypeSeed;
    }

    public Seed getConnectSeed() {
      if (connectSeed == null) {
        connectSeed = SeedUtils.initSeed(0, 2);
      }
      return connectSeed;
    }

    public WhereClauseConfig() {}
  }

  @Data
  public static class SortKeyConfig {
    private Seed keySeed;
    private Seed ifSortSeed;

    public SortKeyConfig(int sortProb) {
      Map<Integer, Integer> sortBuildProb = new HashMap<>();
      // 0 是不排序 1 是排序
      sortBuildProb.put(0, 100 - sortProb);
      sortBuildProb.put(1, sortProb);
      ifSortSeed = SeedUtils.initSeed(sortBuildProb);

      keySeed = SeedUtils.initSeed(0, 1);
    }
  }

  private FromClauseConfig fromClause = new FromClauseConfig();
  private ProjectConfig project = new ProjectConfig();
  private WhereClauseConfig where = new WhereClauseConfig();
  private SortKeyConfig sortKeyConfig;
  private int orderByProbability = 0;
  private Seed lockModeSeed;
  private Map<String, Object> lockMode = null;

  public Seed getLockModeSeed() {
    if (lockModeSeed == null) {
      lockModeSeed = SeedUtils.initSeed(lockMode, OperationLockMode.class);
    }
    return lockModeSeed;
  }

  public SortKeyConfig getSortKeyConfig() {
    if (sortKeyConfig == null) {
      sortKeyConfig = new SortKeyConfig(orderByProbability);
    }
    return sortKeyConfig;
  }

  public OperationConfig() {}
}
