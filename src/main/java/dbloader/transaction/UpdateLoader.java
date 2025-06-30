package dbloader.transaction;

import dbloader.transaction.command.Command;
import dbloader.util.LoaderUtils;
import exception.NoProperRecordFoundException;
import exception.OperationFailException;
import exception.TryLockTimeoutException;
import gen.data.generator.PKGenerator;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import gen.shadow.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import lombok.Cleanup;
import trace.OperationTrace;
import trace.TupleTrace;

public class UpdateLoader extends OperationLoader {
  /**
   * 执行update操作
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
  public static List<Command> loadUpdateOperation(
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
    Table table = (Table) operation.getTable();
    TableMirror tableMirror = loader.getDbMirror().getTableMirrorById(table.getTableId());

    // 初始化中间信息存储结构
    // locked set，这些记录在执行过程中被上锁了，需要在最后解锁
    List<Record> lockedRecordList = new LinkedList<>();
    // 命令列表
    List<Command> commandList = new LinkedList<>();
    List<AttrValue> attrValueList = new ArrayList<>();

    // 找到一个当前存在的动态区或静态区pkId，如果成功返回，对于动态区Record，该记录将处于上锁状态（ReadLock）
    operationTrace.setSql(operation.toSQL());
    int pkId =
        getPkId(
            loader,
            privateRecordMap,
            table,
            new HashSet<>(List.of(PartitionTag.DYNAMIC_EXISTS, PartitionTag.STATIC)));

    Record currentRecord = tableMirror.dynamicRecordMap.get(pkId);

    // trace
    List<TupleTrace> writeTupleTraceList = new ArrayList<>();
    Map<String, String> valueMap = new HashMap<>();
    Map<String, String> realValueMap = new HashMap<>();

    // ForeignKey -> fkId
    Map<ForeignKey, Integer> fk2fkId = new HashMap<>();

    // 静态区
    if (currentRecord == null) {
      // 逐个填参
      int updateColNum = operation.getProject().getBaseAttributeList().size();
      boolean pkIsUpdate = false;

      // 倒过来生成，以保证where部分的主键用旧的数据填充，update部分的主键的新数据不影响它
      for (int i = operation.getParamFillInfoList().size() - 1; i >= 0; i--) {
        ParamInfo fillInfo = operation.getParamFillInfoList().get(i);

        // 需要被update的主键要重新生成
        if (fillInfo.type == ParamType.PK && i < updateColNum && !pkIsUpdate) {
          PKGenerator.pkId2newPk(pkId);
          pkIsUpdate = true;
        }

        AttrValue attrValue =
            generateAttrValue(loader, pkId, fillInfo, fk2fkId, privateRecordMap, lockedRecordList);

        DataType attrType = fillInfo.attr.getAttrType();
        // 设置参数
        LoaderUtils.setParamForPreparedStat(pStat, i + 1, attrType, attrValue);
        attrValueList.add(attrValue);

        // trace
        valueMap.put(fillInfo.getFieldName(), attrValue.value.toString());
      }
      Collections.reverse(attrValueList);

      //            for (ParamInfo fillInfo : operation.getParamFillInfoList()) {
      //                // 确定该参数的值
      //                AttrValue attrValue = generateAttrValue(loader, pkId, fillInfo, fk2fkId,
      // privateRecordMap, lockedRecordList);
      //                DataType attrType = fillInfo.attr.getAttrType();
      //
      //                // 设置参数
      //                LoaderUtils.setParamForPreparedStat(pStat, ++index, attrType, attrValue);
      //                attrValueList.add(attrValue);
      //
      //                // trace
      //                valueMap.put(fillInfo.getFieldName(), attrValue.value.toString());
      //            }

      // 向trace加载结果，包括写集、实例化的SQL、谓词
      TupleTrace tupleTrace =
          TupleTrace.builder()
              .table(String.valueOf(table.getTableName()))
              .primaryKey(String.valueOf(pkId))
              .valueMap(valueMap)
              .realValueMap(realValueMap)
              .build();
      writeTupleTraceList.add(tupleTrace);
      operationTrace.setWriteTupleList(writeTupleTraceList);
      loadSQLInfo(operationTrace, operation, loader, attrValueList);

      // 执行
      int updateCount = executeWriteOperation(operationTrace, pStat);
      if (updateCount == 1) {
        // do nothing
        // 不需要更改 存在状态
        // 添加修改记录
        LoaderUtils.addAlterRecord(alteredRecordMap, table, pkId);
      } else if (updateCount == 0) {
        operationTrace.setWriteTupleList(null);
      } else if (updateCount == table.getTableSize()) {
        writeTupleTraceList.clear();
        for (int id = 0; id < table.getTableSize(); id++) {
          writeTupleTraceList.add(
              TupleTrace.builder()
                  .table(String.valueOf(table.getTableName()))
                  .primaryKey(String.valueOf(id))
                  .valueMap(valueMap)
                  .realValueMap(realValueMap)
                  .build());
          operationTrace.setWriteTupleList(writeTupleTraceList);
          LoaderUtils.addAlterRecord(alteredRecordMap, table, id);
        }
      } else {
        throw new RuntimeException("updateCount of full table update should equal to table size!");
      }

    }
    // 动态区
    else {
      lockedRecordList.add(currentRecord);

      // 构造私有Record
      if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(table, pkId))) {
        privateRecordMap.put(LoaderUtils.priTagKey(table, pkId), currentRecord.copy());
      }
      Record privateRecord = privateRecordMap.get(LoaderUtils.priTagKey(table, pkId));

      try {
        int index = 0;
        for (ParamInfo fillInfo : operation.getParamFillInfoList()) {
          // 确定该参数的值，把和fk关系维护相关的逻辑下推到下一个代码块
          AttrValue attrValue =
              generateAttrValue(
                  loader, pkId, fillInfo, fk2fkId, privateRecordMap, lockedRecordList);
          DataType attrType = fillInfo.attr.getAttrType();

          // 维护外键修改带来的级联处理
          if (fillInfo.type == ParamType.FK) {

            // 参照表的信息
            Table refToTable = fillInfo.fk.getReferencedTable();
            TableMirror newRefToTableMirror =
                loader.getDbMirror().getTableMirrorById(refToTable.getTableId());
            int fkId = fk2fkId.get(fillInfo.fk);

            // 如果fkId不属于静态区，则它不应该为null
            assert PKGenerator.calcPartition(fkId, newRefToTableMirror.getPkPartitionAlg())
                    == PartitionTag.STATIC
                || newRefToTableMirror.dynamicRecordMap.get(fkId) != null;

            // 维护 miniShadow 双向引用关系
            // 现在应当已经获取了当前记录的锁，以及fkId对应记录的锁
            // 类似MVCC，把维护操作分为删除旧纪录和插入新纪录

            // 移除旧的被引用表中的引用信息
            // 这里应当参考 privateRecord，currentRecord的引用信息可能是过时的
            // 可能已经被同一事务中先前的操作给 update 了
            // 但是 DropBiRefCommand 和 CreateBiRefCommand 尚未执行（会延迟到该事务成功才执行）
            // 不过 privateRecord 中的信息是可靠的，先前的Update若成功，则会更新privateRecord中的引用信息
            Reference oldRef =
                privateRecord
                    .getRefInfo()
                    .refFromMap
                    .get(RefInfo.fromKey(fillInfo.table, fillInfo.attr));
            // 如果不为null，那么它应该属于动态区，因为静态区引用不会记录（静态区发出的引用和指向静态区的引用均不会记录）
            // 只有动态区才需要操作，静态区不记录
            if (oldRef != null) {
              String fromKey = RefInfo.fromKey(oldRef.fromTable, oldRef.fromAttr);
              removeRecordForDynamic(
                  loader, currentRecord, privateRecordMap, oldRef, lockedRecordList, commandList);
              privateRecord.getRefInfo().refFromMap.remove(fromKey);
            }

            // 添加新引用关系
            // 只有动态区才需要操作，静态区不记录
            // 获取被参考数据对应的Record，需要注意的是只有动态区数据才有Record，所以它可能为null

            if (newRefToTableMirror.dynamicRecordMap.containsKey(fkId)) {
              addNewRecordForDynamic(
                  loader, pkId, fillInfo, table, privateRecordMap, fkId, commandList);
            }
          }

          // 设置参数
          LoaderUtils.setParamForPreparedStat(pStat, ++index, attrType, attrValue);
          attrValueList.add(attrValue);

          // trace
          valueMap.put(fillInfo.getFieldName(), attrValue.value.toString());
        }

        // 把写集、 实例化SQL、谓词导入trace中
        TupleTrace tupleTrace =
            TupleTrace.builder()
                .table(String.valueOf(table.getTableName()))
                .primaryKey(String.valueOf(pkId))
                .valueMap(valueMap)
                .realValueMap(realValueMap)
                .build();
        writeTupleTraceList.add(tupleTrace);
        operationTrace.setWriteTupleList(writeTupleTraceList);

        loadSQLInfo(operationTrace, operation, loader, attrValueList);

        int updateCount = executeWriteOperation(operationTrace, pStat);
        if (updateCount == 1) {
          // do nothing
          // 不需要更改 存在状态
          // 添加修改记录
          LoaderUtils.addAlterRecord(alteredRecordMap, table, pkId);

        } else if (updateCount == 0) {

          operationTrace.setWriteTupleList(null);

          // 未成功执行，则清空命令列表
          commandList.clear();
        }

        // 把需要的锁添加到事务级别的 needLockRecordSet
        needLockRecordSet.addAll(lockedRecordList);
      } catch (NoProperRecordFoundException | TryLockTimeoutException e) {
        throw new OperationFailException(e);
      } finally {
        // 在此处释放占有的锁
        lockedRecordList.forEach(record -> record.lock.readLock().unlock());
      }
    }
    return commandList;
  }
}
