package gen.operation.basic;

import adapter.SQLFilter;
import config.FaultConfig;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.enums.FaultType;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.schema.Schema;
import gen.schema.table.Table;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import util.rand.RandUtil;

public class OperationFault extends gen.operation.Operation {
  @Getter @Setter private FaultType faultType;

  private String sql;

  public OperationFault(SQLFilter sqlFilter, Schema schema, FaultConfig faultConfig) {
    super(sqlFilter);
    this.setOperationType(OperationType.Fault);
    this.setLockMode(new LockMode(OperationLockMode.SELECT));

    faultType = FaultType.values()[faultConfig.getFaultTypeSeed().getRandomValue()];
    RandUtil randUtil = new RandUtil();

    List<Table> tables = schema.getTableList();
    Table targetTable = tables.get(randUtil.nextInt(tables.size()));

    this.setTable(targetTable);
    this.sql = faultType.toString();
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
