package dbloader.transaction;

import static context.OrcaContext.configColl;

import context.OrcaContext;
import dbloader.transaction.command.Command;
import gen.operation.Operation;
import gen.operation.basic.OperationDistributeSchedule;
import gen.operation.enums.DistributeScheduleType;
import gen.schema.table.Partition;
import gen.schema.table.PartitionType;
import gen.schema.table.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Cleanup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import trace.OperationTrace;
import util.rand.RandUtil;

public class DistributeScheduleLoader extends OperationLoader {

  public static final Logger logger = LogManager.getLogger(DistributeScheduleLoader.class);

  /**
   * 执行update操作
   *
   * @param operation 本次要执行的操作
   * @param conn 数据库连接
   * @param operationTrace 要将trace记录到这个对象中
   * @return 命令列表
   * @throws SQLException SQLException时抛出
   */
  public static synchronized List<Command> loadDistributeScheduleOperation(
      TransactionLoader loader, Operation operation, Connection conn, OperationTrace operationTrace)
      throws SQLException {
    @Cleanup Statement stat = conn.createStatement();

    List<Command> commandList = new LinkedList<>();

    // 初始化表信息
    Table table = (Table) operation.getTable();

    Partition partition = table.getPartition();

    assert partition != null;

    String sqlInstance = operation.toSQL();

    RandUtil randUtil = new RandUtil();

    CopyOnWriteArrayList<Integer> partitionIds =
        (CopyOnWriteArrayList<Integer>) partition.getPartitionIds();
    String rgId = null;
    if (partition.getPartitionSize() > 0) {
      int targetPartitionIdx = randUtil.nextInt(partition.getPartitionSize());
      String targetPartitionId = partition.getPartitionByKey(targetPartitionIdx);

      DistributeScheduleType type =
          ((OperationDistributeSchedule) operation).getDistributeScheduleType();
      // 如果是大于partition的枚举常量，里面都是rg region一类与TDSQL绑定的命令
      if (type.ordinal() > DistributeScheduleType.SplitPartition.ordinal()) {
        int idx = -1;
        if ((new Random().nextInt(10)) < 1) { // 10% 概率选择最近出现过的数据
          Integer tmp =
              loader
                  .getDbMirror()
                  .getTableMirrorById(table.getTableId())
                  .getRandomRecentAccessData();
          if (tmp != null) {
            idx = tmp;
          }
        }

        partition.getPhysicalPartition(conn);
        rgId = partition.getRG(idx);
      }

      switch (type) {
        case ReorganizePartition:
          // alter table %s reorganize partition ? into ( partition ?)
          sqlInstance = sqlInstance.replaceFirst("\\?", targetPartitionId);
          // 生成第二个分区参数
          int nextId =
              (targetPartitionIdx + randUtil.nextInt()) % (10 * partition.getPartitionSize());
          while (partitionIds.contains(nextId)) {
            nextId = (nextId + randUtil.nextInt()) % (10 * partition.getPartitionSize());
          }
          sqlInstance = sqlInstance.replaceFirst("\\?", "p" + nextId);
          partitionIds.set(targetPartitionIdx, nextId);
          break;
        case CoalescePartition:
          // alter table %s coalesce partition ?
          sqlInstance = sqlInstance.replaceFirst("\\?", String.valueOf(1));
          // 虽然实际上可能不是这个分区，但没法指定，所以假设是第一个
          partition.getPartitionIds().remove(0);
          break;
        case MergePartition:
          { // 减少分区
            PartitionType partitionType = partition.getPartitionType();
            int partitionSize = partition.getPartitionSize();
            if (partitionSize > 1) {
              switch (partitionType) {
                case RANGE:
                  {
                    // select a random partition to start, then select two of them
                    int partitionSizeLimit =
                        OrcaContext.configColl.getSchema().getTable().getRecordNumber().getEnd();
                    // range of each partition
                    int step = partitionSizeLimit / partitionSize;

                    String firstPartition = targetPartitionId;
                    String secondPartition = partition.getPartitionByKey(targetPartitionIdx + 1);
                    // alter table %s reorganize partition ?, ? into (
                    //                partition ? values less than (?));

                    try {
                      sqlInstance =
                          sqlInstance
                              .replaceFirst("\\?", firstPartition)
                              .replaceFirst("\\?", secondPartition)
                              .replaceFirst("\\?", firstPartition)
                              .replaceFirst(
                                  "\\?",
                                  String.valueOf(
                                      partition.getPartitionRanges().get(targetPartitionIdx + 1)));
                      if (partition.getPartitionSize() > targetPartitionIdx + 1) {
                        partition.getPartitionIds().remove(targetPartitionIdx + 1);
                      }
                    } catch (Exception e) {
                      logger.warn(e.getMessage());
                    }

                    break;
                  }
                case HASH:
                case KEY:
                default:
                  {
                    // tidb
                    // alter table %s coalesce partition 1
                    partition.getPartitionIds().remove(0);
                    // mysql
                    // sqlInstance = String.format("alter table %s coalesce partition %s;",
                    // tableName, partitionSize);
                    break;
                  }
              }
            }

            break;
          }
        case SplitPartition:
          { // 增加分区
            PartitionType partitionType = partition.getPartitionType();
            int partitionSize = partition.getPartitionSize();
            switch (partitionType) {
              case RANGE:
                {
                  int partitionSizeLimit =
                      OrcaContext.configColl.getSchema().getTable().getRecordNumber().getEnd();
                  int step = partitionSizeLimit / partitionSize;

                  String firstPartition = targetPartitionId;
                  partition
                      .getPartitionIds()
                      .add(targetPartitionIdx + 1, partition.getPartitionSize());
                  String secondPartition = partition.getPartitionByKey(targetPartitionIdx + 1);

                  // pk 生成时每一步的步长
                  int valueStep =
                      configColl.getDataGenerate().getPkParamConfig().getStep().getEnd();

                  // alter table %s reorganize partition ? into (
                  //                    "partition ? values less than (?), partition ? values less
                  // than (?));
                  // 新的range（中间切割的位点是原range减掉步长的一半
                  int newRange =
                      (int)
                          (partition.getPartitionRanges().get(targetPartitionIdx)
                              - step * valueStep * 0.5);
                  sqlInstance =
                      sqlInstance
                          .replaceFirst("\\?", firstPartition)
                          .replaceFirst("\\?", firstPartition)
                          .replaceFirst("\\?", String.valueOf(newRange))
                          .replaceFirst("\\?", secondPartition)
                          .replaceFirst(
                              "\\?",
                              String.valueOf(
                                  partition.getPartitionRanges().get(targetPartitionIdx)));
                  partition.getPartitionRanges().add(targetPartitionIdx, newRange);

                  break;
                }
              case HASH:
              case KEY:
              default:
                {
                  // tidb
                  // alter table %s add partition partitions 1
                  partition.getPartitionIds().add(partition.getPartitionSize());
                  // mysql
                  // sqlInstance = String.format("alter table %s partition by %s(%s) partitions
                  // %s;",
                  //                                            tableName, partitionType,
                  // table.getPartition().getPartitionKey().getAttrName(), partitionSize);
                  break;
                }
            }

            break;
          }
        case RebuildPartition:
          // alter table %s rebuild partition ?
        case RepairPartition:
          // alter table %s repair partition ?
          sqlInstance = sqlInstance.replaceFirst("\\?", targetPartitionId);
          break;
        case AddPartition:
          // alter table %s add partition ( partition ? )
          sqlInstance = sqlInstance.replaceFirst("\\?", "p" + (partition.getPartitionSize() + 1));
          partition.getPartitionIds().add(partition.getPartitionSize());
          break;
        case TransferRGLeader:
          // ALTER INSTANCE TRANSFER LEADER RG rep_group_id TO new_leader_node [FORCE];
          if (rgId == null) {
            break;
          }

          sqlInstance = sqlInstance.replaceFirst("rep_group_id", rgId);
          String newLeaderId = partition.getNonLeaderId(rgId);
          sqlInstance = sqlInstance.replaceFirst("new_leader_node", newLeaderId);
          partition.updateChangedInfo(rgId);
          operationTrace.setException(partition.getAccessPartition(rgId));
          break;
          //                case MigrateRG: // 暂时没配那么多节点，该命令不可用
        case SplitRG:
          // ALTER INSTANCE SPLIT RG rep_group_id [BY splitter] [SET 'left_regions' =
          // left_regions_ids
          // AND 'right_regions' = right_regions_ids] [FORCE];

          if (rgId == null) {
            break;
          }

          sqlInstance = sqlInstance.replaceFirst("rep_group_id", rgId);
          if (sqlInstance.contains("manual-assigned")) {
            List<String> regions = partition.getRegion(rgId);
            int idx = randUtil.nextInt(regions.size());
            List<String> leftRegions = regions.subList(0, idx);
            List<String> rightRegions = regions.subList(idx + 1, regions.size());
            sqlInstance =
                sqlInstance.replaceFirst("left_regions_ids", String.join(", ", leftRegions));
            sqlInstance =
                sqlInstance.replaceFirst("right_regions_ids", String.join(", ", rightRegions));
          }
          partition.updateChangedInfo(rgId);
          operationTrace.setException(partition.getAccessPartition(rgId));
          break;
        case SplitRegion:
          {
            // ALTER INSTANCE SPLIT REGION region_id IN RG rep_group_id [AT KEY key_value] [FORCE];
            if (rgId == null) {
              break;
            }
            List<String> regions = partition.getRegion(rgId);
            int idx = randUtil.nextInt(regions.size());
            String region = regions.get(idx);
            sqlInstance = sqlInstance.replaceFirst("rep_group_id", rgId);
            sqlInstance = sqlInstance.replaceFirst("region_id", region);
            partition.updateChangedInfo(region);
            operationTrace.setException(partition.getAccessPartition(region));
          }
          break;
        case MergeRG:
          // ALTER INSTANCE MERGE RG vanished_rep_group_id INTO RG expanded_rep_group_id;
          if (rgId == null) {
            break;
          }
          String anotherRgId = partition.getAnotherRgId(rgId);
          sqlInstance = sqlInstance.replaceFirst("expanded_rep_group_id", rgId);
          sqlInstance = sqlInstance.replaceFirst("vanished_rep_group_id", anotherRgId);
          partition.updateChangedInfo(rgId);
          partition.updateChangedInfo(anotherRgId);
          operationTrace.setException(partition.getAccessPartition(rgId));
          operationTrace.setException(partition.getAccessPartition(anotherRgId));
          break;
        case MergeRegion:
          {
            // ALTER INSTANCE MERGE REGION region_ids IN RG rep_group_id;
            if (rgId == null) {
              break;
            }
            List<String> regions = partition.getRegion(rgId);
            int start = randUtil.nextInt(regions.size());
            int end = (start + 2) % regions.size();
            if (end < start) {
              int tmp = end;
              end = start;
              start = tmp;
            }

            String mergeRegion = String.join(", ", regions.subList(start, end));
            sqlInstance = sqlInstance.replaceFirst("region_ids", mergeRegion);
            sqlInstance = sqlInstance.replaceFirst("rep_group_id", rgId);

            partition.updateChangedInfo(rgId);
            operationTrace.setException(partition.getAccessPartition(rgId));
          }
          break;
        default:
          break;
      }
    } else {
      ((OperationDistributeSchedule) operation)
          .setDistributeScheduleType(DistributeScheduleType.AddPartition);
      sqlInstance =
          String.format("alter table %s add partition ( partition ? )", table.getTableName());
      sqlInstance = sqlInstance.replaceFirst("\\?", "p" + (partition.getPartitionSize() + 1));
      partition.getPartitionIds().add(partition.getPartitionSize());
      partitionIds.add(partition.getPartitionSize());
    }

    operationTrace.setSql(sqlInstance);
    logger.info(
        "Operation ID :{} ; SQL : {}", operationTrace.getOperationID(), operationTrace.getSql());

    operationTrace.setStartTime(System.nanoTime());
    try {
      stat.execute(sqlInstance);
    } catch (Exception e) {
      logger.warn(e.getMessage());
    }
    operationTrace.setFinishTime(System.nanoTime());

    return commandList;
  }
}
