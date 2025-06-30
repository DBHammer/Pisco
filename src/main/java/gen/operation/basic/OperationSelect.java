package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.OperationConfig;
import gen.operation.basic.from.FromClause;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.basic.orderby.SortKey;
import gen.operation.basic.project.ProjectMode;
import gen.operation.basic.project.ProjectRead;
import gen.operation.basic.where.PredicateLock;
import gen.operation.basic.where.WhereClause;
import gen.operation.enums.*;
import gen.operation.param.ParamInfo;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.Table;
import gen.schema.view.View;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import symbol.Symbol;

public class OperationSelect extends gen.operation.Operation {

  @Getter private List<ParamInfo> selectInfoList;
  private final Map<PredicateLock, ParamInfo> predicate2ParamInfoMap = new HashMap<>();

  public OperationSelect(SQLFilter sqlFilter, Schema schema, OperationConfig operationConfig) {
    super(sqlFilter);
    setOperationType(OperationType.SELECT);

    // 1.生成from的表
    this.setTable(
        FromClause.genFromTable(
            schema,
            operationConfig.getFromClause(),
            FromClauseType.values()[
                operationConfig.getFromClause().getTypeSeed().getRandomValue()]));
    AbstractTable fromClause = this.getTable();
    // 2.生成投影的属性
    this.setProject(
        new ProjectRead(fromClause, operationConfig.getProject(), ProjectMode.AllowAll));

    // 3.生成选择的属性
    this.setWhereClause(new WhereClause(fromClause, operationConfig.getWhere()));

    // 4.生成其他内容，包括锁和排序
    this.setLockMode(new LockMode(operationConfig.getLockModeSeed()));
    this.setSortKey(new SortKey(this.getProject(), operationConfig.getSortKeyConfig()));

    // 设置填充参数所必需的参考信息
    this.setParamFillInfoList(genParamFillInfoList());
    this.setSelectInfoList();
  }

  public OperationSelect(
      SQLFilter sqlFilter,
      Schema schema,
      OperationConfig operationConfig,
      StartType operationType) {
    this(sqlFilter, schema, operationConfig);
    // read only下 取消lock
    if (operationType == StartType.StartTransactionReadOnly
        || operationType == StartType.StartTransactionReadOnlyWithConsistentSnapshot) {
      this.setLockMode(new LockMode());
    }
  }

  public void setSelectInfoList() {
    List<ParamInfo> selectInfoList = new ArrayList<>();

    ProjectRead projectRead = (ProjectRead) this.getProject();
    AbstractTable fromTable = this.getTable();

    for (Attribute attribute : projectRead.getBaseAttributeList()) {
      if (fromTable instanceof Table) {
        if (attribute.getAttrName().contains(Symbol.PK_ATTR_PREFIX)) {
          if (fromTable.isPkAttrOfPk2Fk(attribute)) {
            selectInfoList.add(
                new ParamInfo(
                    (Table) fromTable,
                    fromTable.getPrimaryKey(),
                    fromTable.getPk2ForeignKey(),
                    attribute,
                    PredicateOperator.EQUAL_TO));
          } else {
            selectInfoList.add(
                new ParamInfo(
                    (Table) fromTable,
                    fromTable.getPrimaryKey(),
                    attribute,
                    PredicateOperator.EQUAL_TO));
          }
        } else if (attribute.getAttrName().contains(Symbol.COMM_ATTR_PREFIX)) {
          selectInfoList.add(
              new ParamInfo((Table) fromTable, attribute, PredicateOperator.EQUAL_TO));
        } else if (attribute.getAttrName().contains(Symbol.FK_ATTR_PREFIX)) {
          selectInfoList.add(
              new ParamInfo(
                  (Table) fromTable,
                  fromTable.findCommFKByAttr(attribute),
                  attribute,
                  PredicateOperator.EQUAL_TO));
        } else if (attribute.getAttrName().contains(Symbol.UK_ATTR_PREFIX)) {
          selectInfoList.add(
              new ParamInfo(
                  (Table) fromTable,
                  fromTable.getUniqueKey(),
                  attribute,
                  PredicateOperator.EQUAL_TO));
        }
      } else {
        View view = (View) fromTable;
        ParamInfo paramInfo = view.findParamInfo(attribute);
        selectInfoList.add(paramInfo);
      }
    }
    this.selectInfoList = selectInfoList;
  }

  @Override
  public ParamInfo findParamInfoByPredicate(PredicateLock predicate) {
    return predicate2ParamInfoMap.get(predicate);
  }

  private List<ParamInfo> genParamFillInfoList() {

    List<ParamInfo> paramInfoList = new ArrayList<>();

    AbstractTable fromTable = this.getTable();

    // 加入where的部分
    for (PredicateLock predicate : this.getWhereClause().getPredicates()) {
      Attribute attribute = predicate.getLeftValue();
      int num = predicate.getParameterNumber();
      for (int i = 0; i < num; ++i) {
        if (fromTable instanceof Table) {
          if (attribute.getAttrName().contains(Symbol.PK_ATTR_PREFIX)) {
            if (fromTable.isPkAttrOfPk2Fk(attribute)) {
              paramInfoList.add(
                  new ParamInfo(
                      (Table) fromTable,
                      fromTable.getPrimaryKey(),
                      fromTable.getPk2ForeignKey(),
                      attribute,
                      predicate.getPredicateOperator()));
            } else {
              paramInfoList.add(
                  new ParamInfo(
                      (Table) fromTable,
                      fromTable.getPrimaryKey(),
                      attribute,
                      predicate.getPredicateOperator()));
            }
          } else if (attribute.getAttrName().contains(Symbol.COMM_ATTR_PREFIX)) {
            paramInfoList.add(
                new ParamInfo((Table) fromTable, attribute, predicate.getPredicateOperator()));
          } else if (attribute.getAttrName().contains(Symbol.FK_ATTR_PREFIX)) {
            paramInfoList.add(
                new ParamInfo(
                    (Table) fromTable,
                    fromTable.findCommFKByAttr(attribute),
                    attribute,
                    predicate.getPredicateOperator()));
          } else if (attribute.getAttrName().contains(Symbol.UK_ATTR_PREFIX)) {
            paramInfoList.add(
                new ParamInfo(
                    (Table) fromTable,
                    fromTable.getUniqueKey(),
                    attribute,
                    predicate.getPredicateOperator()));
          }
        } else {
          View view = (View) fromTable;
          ParamInfo paramInfo = view.findParamInfo(attribute);
          paramInfoList.add(paramInfo);
        }
      }

      predicate2ParamInfoMap.put(predicate, paramInfoList.get(paramInfoList.size() - 1));
    }
    return paramInfoList;
  }

  @Override
  public String toSQLProtected() {
    // select ... from ... [where ...] [LOCK MODE] [SORT];
    return String.format(
        "select `%s`, %s from %s %s %s %s;",
        Symbol.PKID_ATTR_NAME,
        getProject().toSQL(),
        getTable().toSQL(),
        getWhereClause().toSQL(),
        getSortKey().toSQL(),
        getLockMode().toSQL());
  }

  @Override
  public OperationLockMode getOperationLockMode() {
    return this.getLockMode().getOperationLockMode();
  }
}
