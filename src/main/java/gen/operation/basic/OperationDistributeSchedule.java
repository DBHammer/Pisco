package gen.operation.basic;

import adapter.SQLFilter;
import config.schema.DDLConfig;
import gen.operation.basic.lockmode.LockMode;
import gen.operation.enums.DistributeScheduleType;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.schema.Schema;
import gen.schema.table.Table;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import util.rand.RandUtil;

public class OperationDistributeSchedule extends gen.operation.Operation {

  @Getter @Setter private DistributeScheduleType distributeScheduleType;

  private String sql;

  public OperationDistributeSchedule(SQLFilter sqlFilter, Schema schema, DDLConfig ddlConfig) {
    super(sqlFilter);
    this.setOperationType(OperationType.DistributeSchedule);
    this.setLockMode(new LockMode(OperationLockMode.UPDATE));

    distributeScheduleType =
        DistributeScheduleType.values()[ddlConfig.getDistributeScheduleTypeSeed().getRandomValue()];
    RandUtil randUtil = new RandUtil();

    List<Table> tables = schema.getTableList();
    Table targetTable = tables.get(randUtil.nextInt(tables.size()));

    this.setTable(targetTable);

    String tableName = targetTable.getTableName();

    switch (distributeScheduleType) {
      case AddPartition:
        sql = String.format("alter table %s add partition ( partition ? )", tableName);
        break;
      case RepairPartition:
        sql = String.format("alter table %s repair partition ?", tableName);
        break;
      case RebuildPartition:
        sql = String.format("alter table %s rebuild partition ?", tableName);
        break;
      case CoalescePartition:
        sql = String.format("alter table %s coalesce partition ?", tableName);
        break;
      case ReorganizePartition:
        sql = String.format("alter table %s reorganize partition ? into ( partition ?)", tableName);
        break;
      case MergePartition:
        switch (targetTable.getPartition().getPartitionType()) {
          case RANGE:
            sql =
                String.format(
                    "alter table %s reorganize partition ?, ? into ("
                        + "partition ? values less than (?));",
                    tableName);
            break;
          case KEY:
          case HASH:
          default: // 除了range其他的都和coalesce语义一样
            sql = String.format("alter table %s coalesce partition 1", tableName);
            break;
        }
        break;
      case SplitPartition:
        switch (targetTable.getPartition().getPartitionType()) {
          case RANGE:
            sql =
                String.format(
                    "alter table %s reorganize partition ? into ("
                        + "partition ? values less than (?), partition ? values less than (?));",
                    tableName);
            break;
          case KEY:
          case HASH:
          default: // 除了range其他的都和add语义一样
            sql = String.format("alter table %s add partition partitions 1", tableName);
            break;
        }
        break;
      case TransferRGLeader:
        // ALTER INSTANCE TRANSFER LEADER RG rep_group_id TO new_leader_node [FORCE];
        sql = "ALTER INSTANCE TRANSFER LEADER RG rep_group_id TO 'new_leader_node'";
        if (randUtil.randBoolByPercent(50)) {
          sql += " FORCE";
        }
        break;
        // 暂时没配那么多节点，该命令不可用
        //            case MigrateRG:
        //                // ALTER INSTANCE MIGRATE RG rep_group_id [MOVE [NODE] rm_node] [TO [NODE]
        // dst_node] [SET] migrate_specification [AND migrate_specification ...];
        //                // node 暂时不指定
        //                sql = "ALTER INSTANCE MIGRATE RG rep_group_id";
        //                if (randUtil.randBoolByPercent(50)){
        //                    sql += " MOVE rm_node";
        //                }
        //                if (randUtil.randBoolByPercent(50)){
        //                    sql += " SET 'new_leader' = new_leader_node";
        //                }
        //                break;
      case SplitRG:
        // ALTER INSTANCE SPLIT RG rep_group_id [BY splitter] [SET 'left_regions' = left_regions_ids
        // AND 'right_regions' = right_regions_ids] [FORCE];
        sql = "ALTER INSTANCE SPLIT RG rep_group_id";
        switch (randUtil.nextInt(10)) {
          case 0:
            sql +=
                " BY 'manual-assigned' SET 'left_regions' = 'left_regions_ids' AND 'right_regions' = 'right_regions_ids'";
            break;
          case 1:
            sql += " BY 'table'";
            break;
          case 2:
            sql += " BY 'size'";
            break;
          case 3:
            sql += " BY 'single-table-splitter'";
            break;
          case 4:
            sql += " BY 'separate-table'";
            break;
          default:
        }
        break;
      case SplitRegion:
        // ALTER INSTANCE SPLIT REGION region_id IN RG rep_group_id [AT KEY key_value] [FORCE];
        // 暂不支持 at key
        sql = "ALTER INSTANCE SPLIT REGION region_id IN RG rep_group_id";
        break;
      case MergeRG:
        // ALTER INSTANCE MERGE RG vanished_rep_group_id INTO RG expanded_rep_group_id;
        sql = "ALTER INSTANCE MERGE RG vanished_rep_group_id INTO RG expanded_rep_group_id;";
        break;
      case MergeRegion:
        // ALTER INSTANCE MERGE REGION region_ids IN RG rep_group_id;
        sql = "ALTER INSTANCE MERGE REGION region_ids IN RG rep_group_id;";
        break;
      default:
        sql = "";
        break;
    }
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
