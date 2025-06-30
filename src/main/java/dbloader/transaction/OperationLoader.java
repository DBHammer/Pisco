package dbloader.transaction;

import context.OrcaContext;
import dbloader.transaction.command.Command;
import dbloader.transaction.command.CreateBiRefCommand;
import dbloader.transaction.command.DropBiRefCommand;
import dbloader.util.LoaderUtils;
import exception.NoProperRecordFoundException;
import exception.TryLockTimeoutException;
import gen.data.generator.AttrGenerator;
import gen.data.generator.FKGenerator;
import gen.data.generator.PKGenerator;
import gen.data.generator.UKGenerator;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import gen.shadow.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import trace.OperationTrace;
import util.jdbc.DataSourceUtils;

public class OperationLoader {

  public static final Logger logger = LogManager.getLogger(OperationLoader.class);

  /**
   * 把SQL和谓词条件写进trace里
   *
   * @param tracer trace
   * @param operation 需要执行的操作
   * @param loader 加载操作的loader，用于生成谓词条件
   * @param attrValueList 操作的参数，用于实例化SQL时填参
   */
  protected static void loadSQLInfo(
      OperationTrace tracer,
      Operation operation,
      TransactionLoader loader,
      List<AttrValue> attrValueList) {
    // 实例化SQL
    String filledSql = DataSourceUtils.getQuotedStatement(operation.toSQL(), attrValueList);

    // operationTrace
    tracer.setSql(filledSql);
    tracer.setPredicateLock(
        loader.getAdapter().writePredicate(loader.getIsolation(), operation, attrValueList));
  }

  /**
   * 执行写操作，记录写操作的发起时间，并返回修改的行数
   *
   * @param tracer trace
   * @param pStat prepare statement，用于实际执行SQL
   * @return 修改的行数
   * @throws SQLException SQL 执行失败相关的异常
   */
  protected static int executeWriteOperation(OperationTrace tracer, PreparedStatement pStat)
      throws SQLException {

    // 开始时间戳
    tracer.setStartTime(System.nanoTime());

    logger.info("Operation ID :" + tracer.getOperationID() + " ; SQL : " + tracer.getSql());

    // 执行
    pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
    return pStat.executeUpdate();
    //            queryFuture = QueryExecutor.executor.submit((Callable<Integer>)
    // pStat::executeUpdate);
    //            return queryFuture.get(
    //                    loader.getLoaderConfig().getQueryTimeout(),
    //                    loader.getLoaderConfig().getQueryTimeoutUnit());
  }

  /**
   * 获取pkid，并在找不到合适id时抛出异常
   *
   * @param loader 用于获取和表、分布相关的配置项
   * @param privateRecordMap 事务持有的私有记录
   * @param table pkid所在的表
   * @param tags 检索表的区域（静态区/动态区）
   * @return 指定区域下符合分布抽取的一个id
   * @throws NoProperRecordFoundException 没有合适id
   */
  protected static int getPkId(
      TransactionLoader loader,
      Map<String, Record> privateRecordMap,
      Table table,
      HashSet<PartitionTag> tags)
      throws NoProperRecordFoundException {
    try {
      return LoaderUtils.findDynamicPkIdByTag(
          table,
          loader.getDbMirror().getTableMirrorById(table.getTableId()),
          privateRecordMap,
          tags,
          loader.getLoaderConfig().getVisitDistribution(),
          loader.getLoaderConfig().getDistributionParams());
    } catch (NoProperRecordFoundException e) {
      throw new NoProperRecordFoundException("未找到合适的可删除参数", e);
    }
  }

  /**
   * 为动态区生成一个有效的fkid，并对参照的pkid加锁
   *
   * @param privateRecordMap 事务私有的记录表，用于寻找真正的fkid
   * @param tableMirror 当前表的记录，用于找一个有效的fkid
   * @param lockedRecordList 加锁的记录表，如果找到的fkid来自动态区，需要给它加锁
   * @param fillInfo 参数信息
   * @param refToTable 参考表
   * @param refToTableMirror 参考表的记录
   * @return 生成的fkid
   * @throws NoProperRecordFoundException 找不到合适的记录
   */
  protected static int fillNewFkForDynamic(
      Map<String, Record> privateRecordMap,
      TableMirror tableMirror,
      List<Record> lockedRecordList,
      ParamInfo fillInfo,
      Table refToTable,
      TableMirror refToTableMirror)
      throws NoProperRecordFoundException {
    int fkId =
        FKGenerator.dynamicFkId(tableMirror.getFkParamById(fillInfo.fk.getId()))
            % refToTable.getTableSize();
    // 向下搜寻静态fkId，其实 findRealStaticFKValue 里面已经包含了此函数，但是这里需要获取真正的 fkId 故 事先运行
    // 该函数当前版本会为相应record上锁
    fkId = LoaderUtils.downForExistingPkId(refToTable, refToTableMirror, fkId, privateRecordMap);

    // 如果来自动态区，应当加入 lockedRecordList
    PartitionTag refToTag = PKGenerator.calcPartition(fkId, refToTableMirror.getPkPartitionAlg());
    if (refToTag != PartitionTag.STATIC) {
      Record refRecord = refToTableMirror.dynamicRecordMap.get(fkId);
      // 存储到lockedRecordSet，以便最后释放锁
      // 由于只获取了一次锁，所有应当在这里加到lockedRecordList，保证只加一次
      lockedRecordList.add(refRecord);
    }

    return fkId;
  }

  /**
   * 从动态区删除一个参照表的记录，相应维护各数据结构，包括各数据表的映射/存储关系，级联的操作命令记录
   *
   * @param loader 加载器，用于获取参照表的数据
   * @param currentRecord 当前数据
   * @param privateRecordMap 本事务的私有数据记录
   * @param ref 参照关系
   * @param lockedRecordList 需要加锁的数据表
   * @param commandList 操作指令集
   * @throws TryLockTimeoutException 无法加锁
   */
  protected static void removeRecordForDynamic(
      TransactionLoader loader,
      Record currentRecord,
      Map<String, Record> privateRecordMap,
      Reference ref,
      List<Record> lockedRecordList,
      List<Command> commandList)
      throws TryLockTimeoutException {
    // 参照表
    TableMirror refToGenModel = loader.getDbMirror().getTableMirrorById(ref.toTable.getTableId());

    // 参照的record
    Record refToRecord = refToGenModel.dynamicRecordMap.get(ref.toFkId);

    // 尝试获取 refToRecord 的锁
    // 如果获取失败，则操作无法继续进行
    // 锁加在全局 record 上
    if (!refToRecord.lock.readLock().tryLock()) {
      throw new TryLockTimeoutException();
    }

    // 成功获取锁，则 将record添加到lockedSet中 且 构造删除双向引用命令
    lockedRecordList.add(refToRecord);

    // 参照关系两端的键值
    String fromKey = RefInfo.fromKey(ref.fromTable, ref.fromAttr);
    String toKey = RefInfo.toKey(ref.fromTable, ref.fromAttr, ref.fromPkId);

    // 把双向删除命令加进操作命令表中，因为这个删除实际上是级联删除，不包含在事务的普通操作中，所以需要额外记录，当操作失败时对应回滚这部分操作
    commandList.add(new DropBiRefCommand(currentRecord, refToRecord, fromKey, toKey));

    // 如果私有记录中不包含这部分数据，构造数据的副本加进去
    if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(ref.toTable, ref.toFkId))) {
      privateRecordMap.put(LoaderUtils.priTagKey(ref.toTable, ref.toFkId), refToRecord.copy());
    }

    // 删除参照关系
    privateRecordMap
        .get(LoaderUtils.priTagKey(ref.toTable, ref.toFkId))
        .getRefInfo()
        .refToMap
        .remove(toKey);
  }

  /**
   * 向动态区添加一个参照表的记录，相应维护各数据结构，包括各数据表的映射/存储关系，级联的操作命令记录
   *
   * @param loader 加载器，用于获取当前数据和参照数据
   * @param pkId 当前数据的pkid
   * @param fillInfo 当前数据的类型等信息
   * @param table 当前表（参照表）
   * @param privateRecordMap 私有数据记录表
   * @param fkId 被参照的fkid
   * @param commandList 操作命令
   */
  protected static void addNewRecordForDynamic(
      TransactionLoader loader,
      int pkId,
      ParamInfo fillInfo,
      Table table,
      Map<String, Record> privateRecordMap,
      int fkId,
      List<Command> commandList) {
    // 当前数据
    Record currentRecord =
        loader.getDbMirror().getTableMirrorById(table.getTableId()).dynamicRecordMap.get(pkId);

    // 被参照数据的信息
    Table refToTable = fillInfo.fk.getReferencedTable();
    Record refToRecord =
        loader.getDbMirror().getTableMirrorById(refToTable.getTableId()).dynamicRecordMap.get(fkId);
    Attribute refToAttr = fillInfo.fk.findReferencedAttrByFkAttr(fillInfo.attr);
    Reference ref = new Reference(table, fillInfo.attr, pkId, refToTable, refToAttr, fkId);

    // 参照关系两端的键值
    String fromKey = RefInfo.fromKey(fillInfo.table, fillInfo.attr);
    String toKey = RefInfo.toKey(fillInfo.table, fillInfo.attr, pkId);

    // 添加建立双向引用命令
    commandList.add(new CreateBiRefCommand(currentRecord, refToRecord, fromKey, toKey, ref));

    // 维护私有Record
    privateRecordMap
        .get(LoaderUtils.priTagKey(table, pkId))
        .getRefInfo()
        .refFromMap
        .put(fromKey, ref);

    // 如果私有记录中不存在这个数据，构造副本加进去
    if (!privateRecordMap.containsKey(LoaderUtils.priTagKey(refToTable, fkId))) {
      privateRecordMap.put(LoaderUtils.priTagKey(refToTable, fkId), refToRecord.copy());
    }
    // 添加依赖关系
    privateRecordMap
        .get(LoaderUtils.priTagKey(refToTable, fkId))
        .getRefInfo()
        .refToMap
        .put(toKey, ref);
  }

  /**
   * 根据数据类型和主键生成对应的属性值
   *
   * @param loader 加载器
   * @param pkId 需要生成数据的主键
   * @param fillInfo 数据的类型等信息
   * @param fk2fkId 外键关系
   * @param privateRecordMap 私有数据记录，仅在生成动态区外键时起效
   * @param lockedRecordList 数据锁信息
   * @return 按需生成的属性值
   * @throws NoProperRecordFoundException 无法生成合适的属性值
   */
  protected static AttrValue generateAttrValue(
      TransactionLoader loader,
      int pkId,
      ParamInfo fillInfo,
      Map<ForeignKey, Integer> fk2fkId,
      Map<String, Record> privateRecordMap,
      List<Record> lockedRecordList)
      throws NoProperRecordFoundException {
    // 属性值
    AttrValue attrValue;
    // 需要填的参数的类型
    DataType attrType = fillInfo.attr.getAttrType();

    // 当前表的数据记录，理论上这个tableMirror应该和loader函数里的一致
    TableMirror tableMirror = loader.getDbMirror().getTableMirrorById(fillInfo.table.getTableId());

    if (fillInfo.type == ParamType.PK) {
      attrValue =
          PKGenerator.pkId2Pk(
              pkId, attrType, tableMirror.getPkParamById(fillInfo.attr.getAttrId()));
    } else if (fillInfo.type == ParamType.ATTR) {
      // 随机确定一个值
      int attrId =
          AttrGenerator.dynamicAttrId(tableMirror.getAttrParamById(fillInfo.attr.getAttrId()));

      // 这个地方要注意attrId的含义，getAttrFuncById中的Id指的是Attribute.AttributeID
      // attrId, fkId, pkId 现在都有两种含义
      attrValue =
          AttrGenerator.attrId2Attr(
              attrId,
              attrType,
              tableMirror.getAttrFuncById(fillInfo.attr.getAttrId()),
              loader.getDbMirror().getDateRepo());

    } else if (fillInfo.type == ParamType.UK) {
      int ukId = UKGenerator.dynamicAttrId(tableMirror.getUKParamById(fillInfo.uk.getId()));
      attrValue =
          UKGenerator.ukId2Attr(
              ukId,
              attrType,
              tableMirror.getAttrFuncById(fillInfo.uk.getId()),
              loader.getDbMirror().getDateRepo());
    } else if (fillInfo.type == ParamType.FK) {

      assert fillInfo.fk != null;
      // 被参照表的信息
      Table refToTable = fillInfo.fk.getReferencedTable();
      Attribute newRefToAttr = fillInfo.fk.findReferencedAttrByFkAttr(fillInfo.attr);
      TableMirror newRefToTableMirror =
          loader.getDbMirror().getTableMirrorById(refToTable.getTableId());

      // 找到一个有效的 fkId，对于每一个Foreign，在一次事务模板填充过程中只产生一次fkId
      if (fk2fkId.get(fillInfo.fk) == null) {
        int fkId;
        // 静态区直接根据规则生成，一定存在
        if (PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg())
            == PartitionTag.STATIC) {
          fkId = LoaderUtils.randStaticPkId(refToTable, newRefToTableMirror);
        } else { // 动态区根据id等约束在当前数据镜像中找存在的fkid
          fkId =
              fillNewFkForDynamic(
                  privateRecordMap,
                  tableMirror,
                  lockedRecordList,
                  fillInfo,
                  refToTable,
                  newRefToTableMirror);
        }
        fk2fkId.put(fillInfo.fk, fkId);
      }
      // 获取已经确定的fkId
      int fkId = fk2fkId.get(fillInfo.fk);

      attrValue =
          PKGenerator.pkId2Pk(
              fkId,
              newRefToAttr.getAttrType(),
              newRefToTableMirror.getPkParamById(newRefToAttr.getAttrId()));
    } else {
      throw new RuntimeException(String.format("fillInfo.type %s 非法", fillInfo.type.name()));
    }

    return attrValue;
  }
}
