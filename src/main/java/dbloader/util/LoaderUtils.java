package dbloader.util;

import config.schema.DistributionType;
import dbloader.distribution.VisitDistribution;
import exception.NoProperRecordFoundException;
import gen.data.generator.PKGenerator;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.schema.table.Table;
import gen.shadow.PartitionTag;
import gen.shadow.Record;
import gen.shadow.TableMirror;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.NotImplementedException;
import util.rand.RandUtils;

public class LoaderUtils {

  /**
   * 找到一个当前存在状态等于tag的记录，如果成功返回，该记录将处于上锁（ReadLock）状态。优先选择近期被调度过的分布式块region
   *
   * @param table table
   * @param tableMirror tableGenModel
   * @param privateRecordMap privateRecordMap
   * @param expectedTagSet expectedTagSet
   * @return pkId
   * @throws NoProperRecordFoundException 如果找不到参数将会抛出该错误
   */
  public static int findDynamicPkIdByTag(
      Table table,
      TableMirror tableMirror,
      Map<String, Record> privateRecordMap,
      Set<PartitionTag> expectedTagSet,
      DistributionType distributionType,
      String distributionParams)
      throws NoProperRecordFoundException {
    Set<Integer> dynamicIdSet = tableMirror.dynamicRecordMap.keySet();

    VisitDistribution distribution = new VisitDistribution(distributionType, distributionParams);

    int pkId = distribution.sample();
    pkId = (pkId + table.getTableSize()) % table.getTableSize();

    if ((new Random().nextInt(10)) < 1) { // 10% 概率选择最近出现过的数据
      Integer tmp = tableMirror.getRandomRecentAccessData();
      if (tmp != null) {
        pkId = tmp;
      }
    }

    int delta = 0;

    // 临近搜索
    while (delta < table.getTableSize()) {
      int upPkId = (pkId + delta + table.getTableSize()) % table.getTableSize();
      int downPkId = (pkId - delta + table.getTableSize()) % table.getTableSize();
      delta += 1;

      // 利用Set避免重复上锁
      Set<Integer> idSet = new HashSet<>(Arrays.asList(upPkId, downPkId));

      for (int id : idSet) {
        // 静态区
        if (expectedTagSet.contains(PartitionTag.STATIC) && !dynamicIdSet.contains(id)) {
          tableMirror.addRecentAccessData(id);
          return id;
        }
        // 动态区
        else if (dynamicIdSet.contains(id)) {
          Record record = tableMirror.dynamicRecordMap.get(id);
          // 尝试上读锁
          // 如果无法上锁则放弃此pkId，直接尝试下一个pkId
          if (!record.lock.readLock().tryLock()) {
            continue;
          }

          // 如果私有tagMap包含该记录，则以私有记录为准
          PartitionTag tag;
          tag = privateRecordMap.getOrDefault(priTagKey(table, id), record).getTag();

          // 找到了一条满足寻找条件的记录，保持上锁状态，退出循环
          if (expectedTagSet.contains(tag)) { // expectedTagSet里是 NOT_EXISTS
            tableMirror.addRecentAccessData(id);
            return id;
          }

          // 解锁
          record.lock.readLock().unlock();
        }
      }
    }

    throw new NoProperRecordFoundException();
  }

  /**
   * 向下搜寻一个当前存在于数据库中的存在的pkId（如果属于动态区，将会为相应Record加ReadLock） 该操作用于动态生成fkId，因此可能会跳过某些无法获取锁的项目，不能保证确定性
   *
   * @return existing pkId（如果属于动态区，将会为相应Record加锁）
   */
  public static int downForExistingPkId(
      Table table, TableMirror tableMirror, int pkId, Map<String, Record> privateTagMap)
      throws NoProperRecordFoundException {

    int tmpPkId;
    for (int i = 0; i < 100; i++) {
      tmpPkId = (pkId + i) % tableMirror.getMaxSize();

      if (isThisPkIdOk(table, tableMirror, tmpPkId, privateTagMap)) {
        return tmpPkId;
      }

      tmpPkId = (pkId - i + tableMirror.getMaxSize()) % tableMirror.getMaxSize();
      if (isThisPkIdOk(table, tableMirror, tmpPkId, privateTagMap)) {
        return tmpPkId;
      }
    }

    throw new NoProperRecordFoundException();
  }

  /**
   * 随机获取一个静态的的pkId 该操作用于生成fkId
   *
   * @return static pkId
   */
  public static int randStaticPkId(Table table, TableMirror tableMirror) {

    // 构造静态集
    Set<Integer> staticFkIdSet =
        IntStream.range(0, table.getTableSize()).boxed().collect(Collectors.toSet());
    staticFkIdSet.removeAll(tableMirror.dynamicRecordMap.keySet());

    return RandUtils.getRandElement(new ArrayList<>(staticFkIdSet));
  }

  private static boolean isThisPkIdOk(
      Table table, TableMirror tableMirror, int pkId, Map<String, Record> privateTagMap) {
    // 静态区直接返回
    if (PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg()) == PartitionTag.STATIC) {
      return true;
    }
    // 如果是动态区数据，获取record
    Record tmpRecord = tableMirror.dynamicRecordMap.get(pkId);
    // 尝试上锁
    // 如果成功上锁检验pkId所代表的record是否存在
    if (tmpRecord.lock.readLock().tryLock()) {
      // 上锁成功，开始检验其是否存在
      // 如果私有tagMap包含该记录，则以私有记录为准
      PartitionTag tag;
      tag = privateTagMap.getOrDefault(priTagKey(table, pkId), tmpRecord).getTag();

      if (tag == PartitionTag.DYNAMIC_EXISTS) {
        return true;
      }

      // 执行到此处则说明该记录不存在，应当解锁
      tmpRecord.lock.readLock().unlock();
    }

    return false;
  }

  /**
   * 生成用于 privateTagMap 的 key
   *
   * @param table 表
   * @param pkId pkId
   * @return key of this record
   */
  public static String priTagKey(Table table, int pkId) {
    return String.format("%s.%d", table.getTableName(), pkId);
  }

  /**
   * 根据类型为PreparedStatement调用合适的方法设置参数
   *
   * @param pStat PreparedStatement
   * @param index index
   * @param attrType attrType
   * @param attrValue attrValue
   * @throws SQLException
   */
  public static void setParamForPreparedStat(
      PreparedStatement pStat, int index, DataType attrType, AttrValue attrValue)
      throws SQLException {
    switch (attrType) {
      case VARCHAR:
        pStat.setString(index, (String) attrValue.value);
        break;
      case INTEGER:
        pStat.setInt(index, (Integer) attrValue.value);
        break;
      case BOOL:
        pStat.setBoolean(index, (Boolean) attrValue.value);
        break;
      case DOUBLE:
      case DECIMAL:
        pStat.setDouble(index, (Double) attrValue.value);
        break;
      case BLOB:
        pStat.setBinaryStream(index, (InputStream) attrValue.value);
        break;
      case TIMESTAMP:
        pStat.setTimestamp(index, (Timestamp) attrValue.value);
        break;
    }
  }

  /**
   * 根据类型读取ResultSet
   *
   * @return value
   */
  public static Object readResultSetByType(ResultSet resultSet, int colInd, DataType type)
      throws SQLException {
    Object columnValue;
    switch (type) {
      case INTEGER:
        columnValue = resultSet.getInt(colInd);
        break;
      case DOUBLE:
      case DECIMAL:
        columnValue = resultSet.getDouble(colInd);
        break;
      case VARCHAR:
        columnValue = resultSet.getString(colInd);
        break;
      case BOOL:
        columnValue = resultSet.getBoolean(colInd);
        break;
      case TIMESTAMP:
        columnValue = resultSet.getTimestamp(colInd);
        break;
      default:
        throw new NotImplementedException();
    }

    return columnValue;
  }

  public static void addAlterRecord(
      Map<Integer, Set<Integer>> alteredRecordMap, int tableId, int pkId) {
    alteredRecordMap.putIfAbsent(tableId, new HashSet<>());
    alteredRecordMap.get(tableId).add(pkId);
  }

  public static void addAlterRecord(
      Map<Integer, Set<Integer>> alteredRecordMap, Table table, int pkId) {
    addAlterRecord(alteredRecordMap, table.getTableId(), pkId);
  }
}
