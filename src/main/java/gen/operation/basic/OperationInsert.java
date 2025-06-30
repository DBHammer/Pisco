package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.OperationConfig;
import gen.operation.Operation;
import gen.operation.basic.from.FromClause;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.basic.orderby.SortKey;
import gen.operation.basic.project.Project;
import gen.operation.basic.project.ProjectMode;
import gen.operation.basic.project.ProjectWrite;
import gen.operation.basic.where.PredicateLock;
import gen.operation.basic.where.WhereClause;
import gen.operation.enums.FromClauseType;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.operation.enums.PredicateOperator;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.*;
import java.util.ArrayList;
import java.util.List;
import symbol.Symbol;

public class OperationInsert extends Operation {

  public OperationInsert(
      SQLFilter sqlFilter,
      Project project,
      AbstractTable table,
      LockMode lockMode,
      SortKey sortKey,
      WhereClause whereClause,
      List<ParamInfo> paramFillInfoList) {
    super(
        OperationType.INSERT,
        sqlFilter,
        project,
        table,
        lockMode,
        sortKey,
        whereClause,
        paramFillInfoList);
  }

  public OperationInsert(SQLFilter sqlFilter, Schema schema, OperationConfig operationConfig) {
    super(sqlFilter);

    // 设置操作类型
    setOperationType(OperationType.INSERT);

    // 1.生成from的表
    this.setTable(
        FromClause.genFromTable(schema, operationConfig.getFromClause(), FromClauseType.TABLE));
    // 2.生成投影的属性
    this.setProject(
        new ProjectWrite(
            this.getTable(), operationConfig.getProject(), ProjectMode.PrimaryKeyOnly));

    // 3.生成选择的属性
    // this.setWhereClause(new WhereClause(fromClause, (Element)
    // operationXML.selectSingleNode("where_clause"), true, 2));
    List<Attribute> nonPKAttributes = new ArrayList<>();
    List<String> groupInfo = new ArrayList<>();
    // 加入普通属性
    for (AttributeGroup attributeGroup : this.getTable().getAttributeGroupList()) {
      nonPKAttributes.addAll(attributeGroup);
      groupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' + attributeGroup.getId());
    }

    // 加入唯一键
    UniqueKey uniqueKey = this.getTable().getUniqueKey();
    nonPKAttributes.addAll(uniqueKey);
    groupInfo.add(Symbol.UK_ATTR_PREFIX + '_' + uniqueKey.getId());

    // 加入普通外键
    for (ForeignKey foreignKey : this.getTable().getCommonForeignKeyList()) {
      nonPKAttributes.addAll(foreignKey);
      groupInfo.add(Symbol.FK_ATTR_PREFIX + '_' + foreignKey.getId());
    }
    this.setWhereClause(new WhereClause(nonPKAttributes, groupInfo));
    // 4.设置填充参数所必需的参考信息
    this.setParamFillInfoList(genParamInfoList());
  }

  /**
   * delete转为insert的构造函数
   *
   * @param operation correspond insert operation
   */
  public OperationInsert(OperationDelete operation) {
    super(operation.getSqlFilter());
    // 设置操作类型
    setOperationType(OperationType.INSERT);
    // 1.获得from的表
    this.setTable(operation.getTable());
    // 2.生成投影的属性，即对应的delete的where内的主键点写
    this.setProject(
        new ProjectWrite(
            operation.getWhereClause().getBaseAttributeList(),
            operation.getWhereClause().getAttributeGroupInfo()));
    // 3.补充选择的属性
    AbstractTable fromClause = operation.getTable();
    List<Attribute> nonPKAttributes = new ArrayList<>();
    List<String> groupInfo = new ArrayList<>();
    // 3.1加入普通属性
    for (AttributeGroup attributeGroup : fromClause.getAttributeGroupList()) {
      nonPKAttributes.addAll(attributeGroup);
      groupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' + attributeGroup.getId());
    }
    // 3.2加入普通外键
    for (ForeignKey foreignKey : fromClause.getCommonForeignKeyList()) {
      nonPKAttributes.addAll(foreignKey);
      groupInfo.add(Symbol.FK_ATTR_PREFIX + '_' + foreignKey.getId());
    }
    // 加入唯一键
    UniqueKey uniqueKey = this.getTable().getUniqueKey();
    nonPKAttributes.addAll(uniqueKey);
    groupInfo.add(Symbol.UK_ATTR_PREFIX + '_' + uniqueKey.getId());

    this.setWhereClause(new WhereClause(nonPKAttributes, groupInfo));

    this.setParamFillInfoList(genParamInfoList());
  }

  private List<ParamInfo> genParamInfoList() {
    List<ParamInfo> paramInfoList = new ArrayList<>();
    Project projectWrite = this.getProject();

    Table fromTable = (Table) this.getTable();

    // pkId
    paramInfoList.add(new ParamInfo(fromTable, ParamType.PkId));

    // 注意，因为insert要求将from和project两个部分的属性都作为插入属性，需要分别把这两个部分加入进去
    // 加入project的部分，只可能是主键
    for (Attribute attribute : projectWrite.getBaseAttributeList()) {
      if (fromTable.isPkAttrOfPk2Fk(attribute)) {
        paramInfoList.add(
            new ParamInfo(
                fromTable,
                fromTable.getPrimaryKey(),
                fromTable.getPk2ForeignKey(),
                attribute,
                PredicateOperator.EQUAL_TO));
      } else {
        paramInfoList.add(
            new ParamInfo(
                fromTable, fromTable.getPrimaryKey(), attribute, PredicateOperator.EQUAL_TO));
      }
    }
    // 加入where的部分，不可能是主键

    for (PredicateLock predicate : this.getWhereClause().getPredicates()) {
      Attribute attribute = predicate.getLeftValue();
      int num = predicate.getParameterNumber();
      for (int i = 0; i < num; ++i) {
        if (attribute.getAttrName().startsWith(Symbol.COMM_ATTR_PREFIX)) {
          paramInfoList.add(new ParamInfo(fromTable, attribute, predicate.getPredicateOperator()));
        } else if (attribute.getAttrName().startsWith(Symbol.FK_ATTR_PREFIX)) {
          paramInfoList.add(
              new ParamInfo(
                  fromTable,
                  fromTable.findCommFKByAttr(attribute),
                  attribute,
                  predicate.getPredicateOperator()));
        } else if (attribute.getAttrName().startsWith(Symbol.UK_ATTR_PREFIX)) {
          paramInfoList.add(
              new ParamInfo(
                  fromTable,
                  fromTable.getUniqueKey(),
                  attribute,
                  predicate.getPredicateOperator()));
        }
      }
    }
    return paramInfoList;
  }

  /**
   * 生成insert的语句
   *
   * @return insert clause in string format
   */
  @Override
  public String toSQLProtected() {

    Project projectWrite = this.getProject();
    ArrayList<String> attributes = new ArrayList<>();

    // pkId
    attributes.add("`pkId`");

    // 注意，因为insert要求将from和project两个部分的属性都作为插入属性，需要分别把这两个部分加入进去
    // 加入project的部分
    for (Attribute attribute : projectWrite.getBaseAttributeList()) {
      attributes.add(String.format("`%s`", attribute.getAttrName()));
    }
    // 加入where的部分
    for (Attribute attribute : this.getWhereClause().getBaseAttributeList()) {
      attributes.add(String.format("`%s`", attribute.getAttrName()));
    }

    // 插入对应的右值，流程同上
    List<String> projectRightValue = this.getProject().getRightValueList();
    List<String> rightValues = new ArrayList<>();

    // pkId
    rightValues.add("?");

    rightValues.addAll(projectRightValue);

    List<PredicateLock> whereClausePredicate = this.getWhereClause().getPredicates();
    for (PredicateLock predicate : whereClausePredicate) {
      rightValues.addAll(predicate.getRightValue());
    }

    return String.format(
        "insert into %s(%s) values(%s);",
        getTable().toSQL(), String.join(", ", attributes), String.join(", ", rightValues));
  }

  @Override
  public OperationLockMode getOperationLockMode() {
    return OperationLockMode.INSERT;
  }
}
