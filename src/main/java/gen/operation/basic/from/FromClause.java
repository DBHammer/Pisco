package gen.operation.basic.from;

import config.schema.OperationConfig;
import gen.operation.enums.FromClauseType;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Table;
import gen.schema.view.View;
import io.SQLizable;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import util.xml.Seed;

@Data
public class FromClause implements SQLizable, Serializable {
  // from clause所基于的abstractTable
  private final AbstractTable fromAbsTable;

  public FromClause(
      Schema schema,
      OperationConfig.FromClauseConfig fromClauseConfig,
      FromClauseType fromClauseType) {
    List<Table> tableList = schema.getTableList();
    List<View> viewList = schema.getViewList();
    //
    if (viewList.isEmpty()) {
      fromClauseType = FromClauseType.TABLE;
    }

    Seed tableSeed = fromClauseConfig.getFromClauseSeed();
    int tableIndex;

    switch (fromClauseType) {
      case TABLE:
        tableSeed.setRange(0, tableList.size());
        fromAbsTable = tableList.get(tableSeed.getRandomValue());
        break;

      case VIEW:
        tableSeed.setRange(0, viewList.size());
        fromAbsTable = viewList.get(tableSeed.getRandomValue());

        break;
      default:
        tableSeed.setRange(0, tableList.size() + viewList.size());
        // 生成一个随机数，以确定是操作哪个表from which table
        tableIndex = tableSeed.getRandomValue();

        // 判断是view 还是 table
        if (tableIndex < tableList.size()) {
          fromAbsTable = tableList.get(tableIndex);
        } else {
          fromAbsTable = viewList.get(tableIndex - tableList.size());
        }
        break;
    }
  }

  public FromClause(AbstractTable table) {
    this.fromAbsTable = table;
  }

  /** 单纯返回表名，不含from字段 */
  public String toString() {
    return toSQL();
  }

  @Override
  public String toSQL() {
    return String.format("`%s`", fromAbsTable.getTableName());
  }

  public static AbstractTable genFromTable(
      Schema schema,
      OperationConfig.FromClauseConfig fromClauseConfig,
      FromClauseType fromClauseType) {
    List<Table> tableList = schema.getTableList();
    List<View> viewList = schema.getViewList();
    //
    if (viewList.isEmpty()) {
      fromClauseType = FromClauseType.TABLE;
    }

    Seed tableSeed = fromClauseConfig.getFromClauseSeed();
    int tableIndex;

    switch (fromClauseType) {
      case TABLE:
        tableSeed.setRange(0, tableList.size());
        return tableList.get(tableSeed.getRandomValue());

      case VIEW:
        tableSeed.setRange(0, viewList.size());
        return viewList.get(tableSeed.getRandomValue());

      default:
        tableSeed.setRange(0, tableList.size() + viewList.size());
        // 生成一个随机数，以确定是操作哪个表from which table
        tableIndex = tableSeed.getRandomValue();

        // 判断是view 还是 table
        if (tableIndex < tableList.size()) {
          return tableList.get(tableIndex);
        } else {
          return viewList.get(tableIndex - tableList.size());
        }
    }
  }
}
