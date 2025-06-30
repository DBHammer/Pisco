package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.OperationConfig;
import gen.operation.basic.from.FromClause;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.basic.orderby.SortKey;
import gen.operation.basic.project.Project;
import gen.operation.basic.where.PredicateLock;
import gen.operation.basic.where.WhereClause;
import gen.operation.enums.FromClauseType;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.operation.param.ParamInfo;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.Table;
import java.util.ArrayList;
import java.util.List;

public class OperationDelete extends gen.operation.Operation {

  public OperationDelete(SQLFilter sqlFilter, Schema schema, OperationConfig operationConfig) {
    super(sqlFilter);
    setOperationType(OperationType.DELETE);

    // 1.生成from的表
    this.setTable(
        FromClause.genFromTable(schema, operationConfig.getFromClause(), FromClauseType.TABLE));
    AbstractTable fromClause = this.getTable();
    // 2.生成选择的属性
    this.setWhereClause(new WhereClause(fromClause, operationConfig.getWhere(), true, 0));

    // 设置填充参数所必需的参考信息
    this.setParamFillInfoList(genParamFillInfoList());
  }

  public OperationDelete(
      SQLFilter sqlFilter, OperationConfig operationConfig, AbstractTable table) {
    super(sqlFilter);
    setOperationType(OperationType.DELETE);
    // 1.生成from的表
    this.setTable(table);
    AbstractTable fromClauseTable = this.getTable();
    // 2.生成选择的属性
    this.setWhereClause(new WhereClause(fromClauseTable, operationConfig.getWhere(), true, 0));

    // 设置填充参数所必需的参考信息
    this.setParamFillInfoList(genParamFillInfoList());
  }

  /**
   * insert转为delete的构造函数
   *
   * @param operation correspond delete operation
   */
  public OperationDelete(OperationInsert operation) {
    super(operation.getSqlFilter());
    setOperationType(OperationType.DELETE);
    this.setTable(operation.getTable());

    // 1.insert的project部分对应delete的where，即对于主键的点写
    this.setWhereClause(
        new WhereClause(
            operation.getProject().getBaseAttributeList(),
            operation.getProject().getAttributeGroupInfo()));
    this.setParamFillInfoList(genParamFillInfoList());
  }

  public OperationDelete(
      SQLFilter sqlFilter,
      Project project,
      AbstractTable table,
      LockMode lockMode,
      SortKey sortKey,
      WhereClause whereClause,
      List<ParamInfo> paramFillInfoList) {
    super(
        OperationType.DELETE,
        sqlFilter,
        project,
        table,
        lockMode,
        sortKey,
        whereClause,
        paramFillInfoList);
  }

  private List<ParamInfo> genParamFillInfoList() {

    List<ParamInfo> paramInfoList = new ArrayList<>();

    Table fromTable = (Table) this.getTable();

    // 加入where的部分
    for (PredicateLock predicate : this.getWhereClause().getPredicates()) {
      Attribute attribute = predicate.getLeftValue();
      int num = predicate.getParameterNumber();
      for (int i = 0; i < num; i++) {
        if (fromTable.getPk2ForeignKey() != null
            && attribute.getAttrId() < fromTable.getPk2ForeignKey().size()) {
          paramInfoList.add(
              new ParamInfo(
                  fromTable,
                  fromTable.getPrimaryKey(),
                  fromTable.getPk2ForeignKey(),
                  attribute,
                  predicate.getPredicateOperator()));
        } else {
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

  @Override
  public String toSQLProtected() {
    // delete from ... [where ...];
    return String.format("delete from %s %s;", getTable().toSQL(), getWhereClause().toSQL());
  }

  @Override
  public OperationLockMode getOperationLockMode() {
    return OperationLockMode.DELETE;
  }
}
