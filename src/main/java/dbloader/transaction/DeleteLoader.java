package dbloader.transaction;

import context.OrcaContext;
import dbloader.transaction.command.Command;
import dbloader.transaction.command.DropBiRefCommand;
import dbloader.transaction.command.SetTagCommand;
import dbloader.util.LoaderUtils;
import dbloader.util.QueryExecutor;
import exception.*;
import gen.data.generator.PKGenerator;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.table.Table;
import gen.shadow.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import trace.OperationTrace;
import trace.TupleTrace;

public class DeleteLoader extends OperationLoader {
  /**
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
  public static List<Command> loadDeleteOperation(
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

    // 中间状态存储结构
    // 命令列表
    List<Command> commandList = new LinkedList<>();
    // locked set，这些记录在执行过程中被上锁了，需要在最后解锁
    List<Record> lockedRecordList = new LinkedList<>();

    // 找到一个当前存在的动态区pkId，如果成功返回，该记录将处于上锁状态（ReadLock）
    operationTrace.setSql(operation.toSQL());
    int pkId =
        getPkId(
            loader, privateRecordMap, table, new HashSet<>(List.of(PartitionTag.DYNAMIC_EXISTS)));

    Record currentRecord = tableMirror.dynamicRecordMap.get(pkId);
    // 将 currentRecord 加入 lockedRecordList
    lockedRecordList.add(currentRecord);

    // trace
    List<TupleTrace> writeTupleTraceList = new ArrayList<>();
    Map<String, String> valueMap = new HashMap<>();
    Map<String, String> realValueMap = new HashMap<>();
    List<AttrValue> attrValueList = new ArrayList<>();

    // 构造私有Record
    if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(table, pkId))) {
      privateRecordMap.put(LoaderUtils.priTagKey(table, pkId), currentRecord.copy());
    }

    // currentRecord 对应的 私有record
    Record privateRecord = privateRecordMap.get(LoaderUtils.priTagKey(table, pkId));

    // 删除引用该行记录的所有数据
    try {
      deleteRefToThisRecord(
          loader,
          table,
          currentRecord,
          conn,
          commandList,
          privateRecordMap,
          lockedRecordList,
          false,
          alteredRecordMap);
    } catch (SQLException
        | DeleteRefException
        | InterruptedException
        | ExecutionException
        | TimeoutException e) {
      // 解锁所有已获取的锁
      lockedRecordList.forEach(record -> record.lock.readLock().unlock());
      throw new OperationFailException(e);
    }

    // 只会出现主键属性
    try {
      // 逐一设置参数
      int index = 0;
      for (ParamInfo fillInfo : operation.getParamFillInfoList()) {
        // 确定该参数的值
        AttrValue attrValue;

        if (fillInfo.type == ParamType.PK) {
          attrValue =
              PKGenerator.pkId2Pk(
                  pkId,
                  fillInfo.attr.getAttrType(),
                  tableMirror.getPkParamById(fillInfo.attr.getAttrId()));
        } else {
          throw new RuntimeException("fillInfo.type 非法");
        }

        // 设置参数
        LoaderUtils.setParamForPreparedStat(pStat, ++index, fillInfo.attr.getAttrType(), attrValue);
        attrValueList.add(attrValue);

        // 由于 delete 操作，基于主键，无需向valueMap添加元素
        realValueMap.put(fillInfo.attr.getAttrName(), attrValue.value.toString());
      }

      // trace，包括写集、实例化SQL
      writeTupleTraceList.add(
          new TupleTrace(
              String.valueOf(table.getTableName()), String.valueOf(pkId), valueMap, realValueMap));
      operationTrace.setWriteTupleList(writeTupleTraceList);
      loadSQLInfo(operationTrace, operation, loader, attrValueList);

      // 删除该行数据发出的引用关系
      // 当然，仍然需要双向删除
      // 参考 privateRecord
      for (Reference ref : privateRecord.getRefInfo().refFromMap.values()) {
        removeRecordForDynamic(
            loader, currentRecord, privateRecordMap, ref, lockedRecordList, commandList);
      }

      // 维护私有Record
      // 清空 privateRecord.getRefInfo().refFromMap
      privateRecord.getRefInfo().refFromMap.clear();

      // 实际执行删除
      int updateCount = executeWriteOperation(operationTrace, pStat);
      if (updateCount == 1) {
        // 添加 setTag 命令
        commandList.add(new SetTagCommand(currentRecord, PartitionTag.DYNAMIC_NOT_EXISTS));

        // 修改 privateTagMap
        privateRecord.setTag(PartitionTag.DYNAMIC_NOT_EXISTS);

        // 添加修改记录
        LoaderUtils.addAlterRecord(alteredRecordMap, table, pkId);
      } else if (updateCount == 0) {
        // 未删除，则清空
        operationTrace.setWriteTupleList(null);
        //
        //                // 在这种情况下必须回滚操作，因为可能已经级联删除了一部分数据
        //                // 那些数据是因为要删除此行数据而被级联删除的
        //                // 既然此行数据的删除失败了
        //                // 那么那些级联删除也就不应该发生了
        //                // 这里抛出错误，由catch语句块负责解锁
        throw new DeleteFailException();

        // 添加 setTag 命令
        //                commandList.add(new SetTagCommand(currentRecord,
        // PartitionTag.DYNAMIC_NOT_EXISTS));

        // 修改 privateTagMap
        //                privateRecord.setTag(PartitionTag.DYNAMIC_NOT_EXISTS);
      }

      // 把需要的锁添加到事务级别的 needLockRecordSet
      needLockRecordSet.addAll(lockedRecordList);
    } catch (TryLockTimeoutException | DeleteFailException e) {
      // 该操作无法执行，则commandList无法正常返回
      throw new OperationFailException(e);
    } finally {
      // 在此处释放占有的 ReadLock
      lockedRecordList.forEach(record -> record.lock.readLock().unlock());
    }

    return commandList;
  }

  /**
   * 删除引用此行数据的所有数据
   *
   * @param table 该record所属表
   * @param record record
   * @param conn 数据库连接
   * @param privateRecordMap privateRecordMap
   * @param lockedRecordList lockedRecordSet
   * @param selfDelete 是否删除该record关联的数据库数据
   * @throws SQLException 不知道何时会抛出，写这个是因为不写ide会提示，很烦人
   */
  private static void deleteRefToThisRecord(
      TransactionLoader loader,
      Table table,
      Record record,
      Connection conn,
      List<Command> commandList,
      Map<String, Record> privateRecordMap,
      List<Record> lockedRecordList,
      boolean selfDelete,
      Map<Integer, Set<Integer>> alteredRecordMap)
      throws SQLException, DeleteRefException, InterruptedException, ExecutionException,
          TimeoutException {

    // 如果该记录不存在于 privateRecordMap，则 clone
    if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(table, record.getPkId()))) {
      privateRecordMap.put(LoaderUtils.priTagKey(table, record.getPkId()), record.copy());
    }

    // 获取该record 对应的 私有record
    Record privateRecord = privateRecordMap.get(LoaderUtils.priTagKey(table, record.getPkId()));

    // 使用 privateRecord 的 refInfo，进行级联删除
    // 因为 record  可能并不能反映目前实际状态
    for (Reference ref : privateRecord.getRefInfo().refToMap.values()) {
      TableMirror fromTableMirror =
          loader.getDbMirror().getTableMirrorById(ref.fromTable.getTableId());

      // 这是全局record
      Record fromRecord = fromTableMirror.dynamicRecordMap.get(ref.fromPkId);

      // 需要加锁，避免其它线程修改此record
      // 如果获取失败，则操作无法继续进行
      // 锁要加在全局record上
      // 加读锁
      try {
        if (!fromRecord.lock.readLock().tryLock()) {
          throw new TryLockTimeoutException();
        }
      } catch (TryLockTimeoutException e) {
        throw new DeleteRefException(e);
      }

      // 成功获取锁，则 将record添加到lockedSet中
      lockedRecordList.add(fromRecord);

      // 删除所有指向 fromRecord 的 记录
      // selfDelete 为 true，表示删除 fromRecord 所关联的数据本身
      deleteRefToThisRecord(
          loader,
          ref.fromTable,
          fromRecord,
          conn,
          commandList,
          privateRecordMap,
          lockedRecordList,
          true,
          alteredRecordMap);
    }

    // clear HashMap
    // 原来的执行流程是，在删除数据时同时反向删除
    // 但是由于在递归过程中发生了 ConcurrentModificationException
    // 故不再进行反向删除
    // 改为在此处 clear
    privateRecord.getRefInfo().refToMap.clear();

    if (selfDelete) {
      // 执行 SQL 删除此条数据
      deleteRecord(loader, table, record, conn, alteredRecordMap);

      // 构造删除双向引用命令
      // 删除该行数据发出的引用关系
      // 当然，仍然需要双向删除
      // 为什么要用 privateRecord？
      // 假如在执行此Delete操作之前，执行了一个Update操作，更新了一个外键
      // 那么Update执行结束后
      // 数据库中已经 该行记录的引用关系已经更新了
      // 并且此刻已经存在一个 DropBiRefCommand 用于删除旧的引用，但是它还未执行
      // 但是Update操作也会更新对应的privateRecord，该privateRecord才真实的反映了数据库此刻的状态
      for (Reference ref : privateRecord.getRefInfo().refFromMap.values()) {
        String fromKey = RefInfo.fromKey(ref.fromTable, ref.fromAttr);
        String toKey = RefInfo.toKey(ref.fromTable, ref.fromAttr, ref.fromPkId);

        TableMirror refToGenModel =
            loader.getDbMirror().getTableMirrorById(ref.toTable.getTableId());

        // 这是全局 record
        Record refToRecord = refToGenModel.dynamicRecordMap.get(ref.toFkId);

        // 尝试获取 refToRecord 的锁
        // 如果获取失败，则操作无法继续进行
        // 锁加在全局 record 上
        try {
          if (!refToRecord.lock.readLock().tryLock(1, TimeUnit.MICROSECONDS)) {
            throw new TryLockTimeoutException();
          }
        } catch (InterruptedException | TryLockTimeoutException e) {
          throw new DeleteRefException(e);
        }

        // 成功获取锁，则 将record添加到lockedSet中 并 构造删除双向引用命令
        lockedRecordList.add(refToRecord);
        commandList.add(new DropBiRefCommand(record, refToRecord, fromKey, toKey));

        //                // 维护 privateRecordMap
        // 不能在迭代过程中删除元素
        //                privateRecordMap
        //                        .get(LoaderUtils.priTagKey(table, record.getPkId()))
        //                        .getRefInfo()
        //                        .refFromMap
        //                        .remove(fromKey);

        // 这个步骤可能是不必要的，按理说程序能走到这里，必然是从 refToTable 的级联删除操作过来的
        //                if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(ref.refToTable,
        // refToRecord.getPkId()))) {
        //                    privateRecordMap.put(
        //                            LoaderUtils.priTagKey(ref.refToTable, refToRecord.getPkId()),
        //                            record.copy());
        //                }

        // 在此处执行会出事儿
        // 其原因是上一级程序中有一个迭代过程，这里删除就相当于在迭代HashMap的过程中进行了元素删除
        // 故删除这里的反向删除代码，改为在迭代结束之后对hashMap执行clear操作
        //                privateRecordMap
        //                        .get(LoaderUtils.priTagKey(ref.toTable, refToRecord.getPkId()))
        //                        .getRefInfo()
        //                        .refToMap
        //                        .remove(toKey);
      }

      privateRecordMap
          .get(LoaderUtils.priTagKey(table, record.getPkId()))
          .getRefInfo()
          .refFromMap
          .clear();

      // 添加 setTag 命令
      commandList.add(new SetTagCommand(record, PartitionTag.DYNAMIC_NOT_EXISTS));

      // 维护 privateRecordMap
      privateRecordMap
          .get(LoaderUtils.priTagKey(table, record.getPkId()))
          .setTag(PartitionTag.DYNAMIC_NOT_EXISTS);
    }
  }

  /**
   * 删除Record
   *
   * @param table 表
   * @param record record
   * @param conn 数据库连接
   * @throws SQLException SQLException
   */
  private static void deleteRecord(
      TransactionLoader loader,
      Table table,
      Record record,
      Connection conn,
      Map<Integer, Set<Integer>> alteredRecordMap)
      throws SQLException, InterruptedException, ExecutionException, TimeoutException {

    String delTmpl = "delete from %s where \"pkId\"=%d;";
    String delSql = String.format(delTmpl, table.getTableName(), record.getPkId());
    @Cleanup Statement statement = conn.createStatement();
    statement.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
    Future<Integer> queryFuture =
        QueryExecutor.executor.submit(() -> statement.executeUpdate(delSql));
    int updateCount;
    try {
      updateCount =
          queryFuture.get(
              loader.getLoaderConfig().getQueryTimeout(),
              loader.getLoaderConfig().getQueryTimeoutUnit());
    } catch (TimeoutException e) {
      queryFuture.cancel(true);
      throw e;
    }

    //        TransactionLoader.logger.info(String.format("Logger-%d> 级联删除 %s",
    // loader.getLoaderId(), delSql));

    if (updateCount != 0) {
      // 添加修改记录
      LoaderUtils.addAlterRecord(alteredRecordMap, table, record.getPkId());
    }
  }
}
