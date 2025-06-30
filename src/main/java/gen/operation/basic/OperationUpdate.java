package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.OperationConfig;
import gen.operation.Operation;
import gen.operation.basic.from.FromClause;
import gen.operation.basic.project.Project;
import gen.operation.basic.project.ProjectMode;
import gen.operation.basic.project.ProjectWrite;
import gen.operation.basic.where.PredicateLock;
import gen.operation.basic.where.WhereClause;
import gen.operation.enums.*;
import gen.operation.param.ParamInfo;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.PrimaryKey;
import gen.schema.table.Table;
import java.util.ArrayList;
import java.util.List;

public class OperationUpdate extends Operation {
  public OperationUpdate(SQLFilter sqlFilter, Schema schema, OperationConfig operationConfig) {
    super(sqlFilter);
    setOperationType(OperationType.UPDATE);

    // 1.生成from的表
    this.setTable(
        FromClause.genFromTable(schema, operationConfig.getFromClause(), FromClauseType.TABLE));
    AbstractTable fromClause = this.getTable();

    // 3.生成选择的属性
    UpdateType updateRange =
        UpdateType
            .PART; // UpdateType.values()[operationConfig.getUpdateRangeSeed().getRandomValue()];
    this.setWhereClause(new WhereClause(fromClause, operationConfig.getWhere(), true, 0));

    // 2.生成投影的属性
    this.setProject(
        new ProjectWrite(
            fromClause,
            operationConfig.getProject(),
            operationConfig.isUpdatePK() ? ProjectMode.AllowAll : ProjectMode.NoPrimaryKey));

    // 设置填充参数所必需的参考信息
    this.setParamFillInfoList(genParamFillInfoList());
  }

  private List<ParamInfo> genParamFillInfoList() {

    List<ParamInfo> paramInfoList = new ArrayList<>();
    Project projectWrite = this.getProject();

    Table fromTable = (Table) this.getTable();
    for (Attribute attribute : projectWrite.getBaseAttributeList()) {

      PrimaryKey pk = fromTable.getPrimaryKey();
      ForeignKey fk = fromTable.findCommFKByAttr(attribute);
      // 是外键的话
      if (fk != null) {
        paramInfoList.add(new ParamInfo(fromTable, fk, attribute, PredicateOperator.EQUAL_TO));
      }
      // 普通属性
      else if (pk.contains(attribute)) {
        paramInfoList.add(new ParamInfo(fromTable, pk, attribute, PredicateOperator.EQUAL_TO));
      } else {
        paramInfoList.add(new ParamInfo(fromTable, attribute, PredicateOperator.EQUAL_TO));
      }
    }
    // 加入where的部分，同样只可能是主键
    if (this.getWhereClause() != null) {
      for (PredicateLock predicate : this.getWhereClause().getPredicates()) {
        Attribute attribute = predicate.getLeftValue();
        int num = predicate.getParameterNumber();
        for (int i = 0; i < num; i++) {
          paramInfoList.add(
              new ParamInfo(
                  fromTable,
                  fromTable.getPrimaryKey(),
                  attribute,
                  predicate.getPredicateOperator()));
        }
      }
    }

    return paramInfoList;
  }

  /**
   * update语句
   *
   * @return update clause in string format
   */
  @Override
  public String toSQLProtected() {
    return String.format(
        "update %s set %s %s;",
        getTable().toSQL(),
        getProject().toSQL(),
        getWhereClause() == null ? "" : getWhereClause().toSQL());
  }

  @Override
  public OperationLockMode getOperationLockMode() {
    return OperationLockMode.UPDATE;
  }
}
