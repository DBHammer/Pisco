package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.DDLConfig;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.enums.DDLType;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.schema.Schema;
import gen.schema.table.Table;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import util.rand.RandUtil;

public class OperationDDL extends gen.operation.Operation {

  @Getter @Setter private DDLType ddlType;

  private String sql;

  public OperationDDL(SQLFilter sqlFilter, Schema schema, DDLConfig ddlConfig) {
    super(sqlFilter);
    this.setOperationType(OperationType.DDL);
    this.setLockMode(new LockMode(OperationLockMode.UPDATE));

    ddlType = DDLType.values()[ddlConfig.getDDLTypeSeed().getRandomValue()];
    RandUtil randUtil = new RandUtil();

    List<Table> tables = schema.getTableList();
    Table targetTable = tables.get(randUtil.nextInt(tables.size()));

    this.setTable(targetTable);
    this.sql = ddlType.toString();
  }

  /**
   * 事务的起始语句
   *
   * @return 返回不含参的sql
   */
  @Override
  public String toSQLProtected() {
    return sql;
  }
}
