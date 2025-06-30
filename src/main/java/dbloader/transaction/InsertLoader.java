package dbloader.transaction;

import dbloader.transaction.command.Command;
import dbloader.transaction.command.SetTagCommand;
import dbloader.util.LoaderUtils;
import exception.NoProperRecordFoundException;
import exception.OperationFailException;
import gen.data.generator.PKGenerator;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import gen.shadow.PartitionTag;
import gen.shadow.Record;
import gen.shadow.TableMirror;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import lombok.Cleanup;
import trace.OperationTrace;
import trace.TupleTrace;

public class InsertLoader extends OperationLoader {
  /**
   * 加载插入操作
   *
   * @param operation 本次要执行的操作
   * @param conn 数据库连接
   * @param operationTrace 要将trace记录到这个对象中
   * @param privateRecordMap 事务共享的私有Record Map，实时更新该事务所做修改
   * @param needLockRecordSet 事务共享的需要加锁的Record集合，该操作将追加它需要锁的Record至needLockRecordSet
   * @return 命令列表
   * @throws SQLException SQLException时抛出
   * @throws OperationFailException 因为获取锁超时等原因而无法执行操作时抛出
   * @throws NoProperRecordFoundException 找不到合适的操作参数时抛出
   */
  public static List<Command> loadInsertOperation(
      TransactionLoader loader,
      Operation operation,
      Connection conn,
      OperationTrace operationTrace,
      Map<String, Record> privateRecordMap,
      Set<Record> needLockRecordSet,
      Map<Integer, Set<Integer>> alteredRecordMap)
      throws SQLException, OperationFailException, NoProperRecordFoundException {
    @Cleanup PreparedStatement pStat = conn.prepareStatement(operation.toSQL());

    // 初始化表信息
    // 对于insert来说，只考虑from即可
    // 不过对于其他类型的Operation，参数来源可能就比较复杂了
    Table table = (Table) operation.getTable();
    TableMirror tableMirror = loader.getDbMirror().getTableMirrorById(table.getTableId());

    // 初始化中间状态记录
    // locked set，这些记录在执行过程中被上锁了，需要在最后解锁
    List<Record> lockedRecordList = new LinkedList<>();
    // 命令列表
    List<Command> commandList = new LinkedList<>();
    // ForeignKey -> fkId
    Map<ForeignKey, Integer> fk2fkId = new HashMap<>();

    // 生成一个 pkId，所有的插入属性都将根据该 pkId 计算得到
    // 找到一个可以插入的pkId，如果成功返回，该记录将处于上锁状态（ReadLock）
    operationTrace.setSql(operation.toSQL());
    int pkId =
        getPkId(
            loader,
            privateRecordMap,
            table,
            new HashSet<>(List.of(PartitionTag.DYNAMIC_NOT_EXISTS)));
    Record currentRecord = tableMirror.dynamicRecordMap.get(pkId);
    // 将 currentRecord 添加到 lockedRecordSet
    lockedRecordList.add(currentRecord);

    // 构造私有Record
    if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(table, pkId))) {
      privateRecordMap.put(LoaderUtils.priTagKey(table, pkId), currentRecord.copy());
    }

    // 初始化trace信息
    // trace
    List<TupleTrace> writeTupleTraceList = new ArrayList<>();
    Map<String, String> valueMap = new HashMap<>();
    Map<String, String> realValueMap = new HashMap<>();
    List<AttrValue> attrValueList = new ArrayList<>();

    try {

      // 逐一确定参数值
      // 当前确定的参数位置
      int index = 0;
      for (ParamInfo fillInfo : operation.getParamFillInfoList()) {
        // 确定该参数的值
        AttrValue attrValue;
        DataType attrType;

        // pkid一定是int，事实上就是pkid
        if (fillInfo.type == ParamType.PkId) {
          attrType = DataType.INTEGER;
          attrValue = new AttrValue(DataType.INTEGER, pkId);
        } else { // 其他情况统一生成，在这个过程中把涉及的数据加进私有的数据表里并加锁，涉及的数据结构只会增加不会删除会修改
          attrType = fillInfo.attr.getAttrType();
          attrValue =
              generateAttrValue(
                  loader, pkId, fillInfo, fk2fkId, privateRecordMap, lockedRecordList);
        }

        // 独立处理外键修改导致的级联操作
        if (fillInfo.type == ParamType.FK) {
          // 获取参照表的信息
          Table refToTable = fillInfo.fk.getReferencedTable();
          TableMirror refToTableMirror =
              loader.getDbMirror().getTableMirrorById(refToTable.getTableId());
          int fkId = fk2fkId.get(fillInfo.fk);
          // 维护 miniShadow
          // 只有动态区对动态区的引用才记录引用信息
          // 程序运行到这里pkId应当属于动态区
          // 因此需要判断被引用的fkId对应记录是否属于动态区
          // 若属于动态区，则记录双向引用，否则无需记录
          if (PKGenerator.calcPartition(fkId, refToTableMirror.getPkPartitionAlg())
              != PartitionTag.STATIC) {
            addNewRecordForDynamic(
                loader, pkId, fillInfo, table, privateRecordMap, fkId, commandList);
          }
        }

        // 设置参数
        LoaderUtils.setParamForPreparedStat(pStat, ++index, attrType, attrValue);
        attrValueList.add(attrValue);

        // 更新trace
        valueMap.put(fillInfo.getFieldName(), attrValue.value.toString());
      }

      // operationTrace
      writeTupleTraceList.add(
          new TupleTrace(
              String.valueOf(table.getTableName()), String.valueOf(pkId), valueMap, realValueMap));
      operationTrace.setWriteTupleList(writeTupleTraceList);
      // 更新trace中的sql和谓词信息
      loadSQLInfo(operationTrace, operation, loader, attrValueList);

      // 执行写操作
      int updateCount = executeWriteOperation(operationTrace, pStat);
      // 如果执行成功，则修改Record中此条数据的状态
      if (updateCount == 1) {
        commandList.add(new SetTagCommand(currentRecord, PartitionTag.DYNAMIC_EXISTS));

        // 修改 privateTagMap
        // 维护私有Record
        privateRecordMap
            .get(LoaderUtils.priTagKey(table, pkId))
            .setTag(PartitionTag.DYNAMIC_EXISTS);

        // 添加修改记录
        LoaderUtils.addAlterRecord(alteredRecordMap, table, pkId);
      } else if (updateCount == 0) {
        // 未插入，则清空
        operationTrace.setWriteTupleList(null);
        // 如果未能成功执行，则清空此前添加的命令
        commandList.clear();
      }

      // 把需要的锁添加到事务级别的 needLockRecordSet
      needLockRecordSet.addAll(lockedRecordList);
    } catch (NoProperRecordFoundException e) {
      throw new OperationFailException(e);
    } finally {
      // 无论成功与否，都需要在此处释放掉所有的 readLock
      lockedRecordList.forEach(record -> record.lock.readLock().unlock());
    }

    return commandList;
  }
}
