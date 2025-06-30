package dbloader.transaction;

import context.OrcaContext;
import dbloader.transaction.predicate.*;
import dbloader.util.LoaderUtils;
import gen.data.generator.FKGenerator;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.basic.OperationSelect;
import gen.operation.basic.where.PredicateLock;
import gen.operation.enums.PredicateOperator;
import gen.operation.param.ParamInfo;
import gen.operation.param.ParamType;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Table;
import gen.shadow.TableMirror;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import trace.OperationTrace;
import trace.TupleTrace;
import util.rand.RandUtils;

public class SelectLoader extends OperationLoader {
  /**
   * @param operation 本次要执行的操作
   * @param conn 数据库连接
   * @param operationTrace 要将trace记录到这个对象中
   * @throws SQLException SQLException时抛出
   */
  public static void loadSelectOperation(
      TransactionLoader loader, Operation operation, Connection conn, OperationTrace operationTrace)
      throws SQLException {
    @Cleanup PreparedStatement pStat = conn.prepareStatement(operation.toSQL());
    List<AttrValue> attrValueList = new ArrayList<>();

    List<PredGeneric> predList = new ArrayList<>();
    List<PredicateLock> predicateList = operation.getWhereClause().getPredicates();
    List<String> connectors = operation.getWhereClause().getConnect();

    for (PredicateLock predicate : predicateList) {
      ParamInfo paramInfo = operation.findParamInfoByPredicate(predicate);
      AbstractTable fromTable = paramInfo.table;
      TableMirror tableMirror = loader.getDbMirror().getTableMirrorById(fromTable.getTableId());

      // 确定填参的值的范围
      int upper;

      if (paramInfo.type == ParamType.PK) {
        // 主键的范围是表大小
        upper = tableMirror.getMaxSize();
      } else if (paramInfo.type == ParamType.ATTR || paramInfo.type == ParamType.UK) {
        // 属性列的范围是对应属性生成的数值数量
        upper = loader.getDbMirror().getDateRepo().getListSizeByType(paramInfo.attr.getAttrType());
      } else if (paramInfo.type == ParamType.FK) {
        assert paramInfo.fk != null;
        // 外键的范围是参考表的大小
        upper = paramInfo.fk.getReferencedTable().getTableSize();
      } else {
        throw new RuntimeException("fillInfo.type 非法");
      }

      if (predicate.getPredicateOperator() == PredicateOperator.BETWEEN) {
        int leftValue;
        int betweenLength = loader.getLoaderConfig().getBetweenLength();
        // 确定该参数的值
        if (paramInfo.type == ParamType.PK
            || paramInfo.type == ParamType.ATTR
            || paramInfo.type == ParamType.UK) {
          if (upper > betweenLength) { // 生成左边界时给定长的间距留出距离
            leftValue = RandUtils.nextInt(upper - betweenLength);
          } else {
            leftValue = RandUtils.nextInt(upper);
          }
        } else {
          leftValue = FKGenerator.dynamicFkId(tableMirror.getFkParamById(paramInfo.fk.getId()));
        }

        // 右边界显然至少等于左边界，所以算个偏移量就行
        int rightValue = leftValue;
        if (upper > betweenLength) { // 右边界直接根据间距确定
          rightValue += betweenLength;
        } else { // 不然就根据左边界到上界的区间随机生成
          rightValue += RandUtils.nextInt(upper - leftValue);
        }

        predList.add(
            new PredBetweenAnd(
                paramInfo.attr.getAttrName(), paramInfo, leftValue, rightValue, upper));
      } else {
        for (int i = 0; i < predicate.getRightValue().size(); i++) {
          predList.add(generateSinglePred(predicate, paramInfo, upper));
        }
      }
    }

    boolean isValid = PredicateChecker.check(predList, connectors);
    operationTrace.setIsWhereValid(isValid);

    int ind = 0;
    for (PredGeneric pred : predList) {
      for (Integer logicalValue : pred.getLogicalValues()) {
        AttrValue attrValue =
            loader.getDbMirror().getAttrValueByParamInfo(pred.getParamInfo(), logicalValue);
        LoaderUtils.setParamForPreparedStat(
            pStat, ++ind, pred.getParamInfo().attr.getAttrType(), attrValue);
        attrValueList.add(attrValue);
      }
    }

    loadSQLInfo(operationTrace, operation, loader, attrValueList);

    try {
      loadReadTupleList(operationTrace, operation, loader, pStat);
    } catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      }
      logger.warn(e);
    }
  }

  private static PredGeneric generateSinglePred(
      PredicateLock predicate, ParamInfo paramInfo, int upper) {
    int logicalValue = RandUtils.nextInt(upper);
    switch (predicate.getPredicateOperator()) {
      case LESS_THAN:
        return new PredLess(paramInfo.attr.getAttrName(), paramInfo, logicalValue, upper);
      case LESS_EQUAL:
        return new PredLessEqual(paramInfo.attr.getAttrName(), paramInfo, logicalValue, upper);
      case GREATER_THAN:
        return new PredGreater(paramInfo.attr.getAttrName(), paramInfo, logicalValue, upper);
      case GREATER_EQUAL:
        return new PredGreaterEqual(paramInfo.attr.getAttrName(), paramInfo, logicalValue, upper);
      case EQUAL_TO:
      default:
        return new PredEqual(paramInfo.attr.getAttrName(), paramInfo, logicalValue, upper);
    }
  }

  private static void loadReadTupleList(
      OperationTrace operationTrace,
      Operation operation,
      TransactionLoader loader,
      PreparedStatement pStat)
      throws Exception {

    pStat.setQueryTimeout(OrcaContext.configColl.getLoader().getQueryTimeout());
    // readTupleList
    List<TupleTrace> readTupleTraceList = new ArrayList<>();

    List<ParamInfo> selectInfoList = ((OperationSelect) operation).getSelectInfoList();
    // 操作生成时间戳
    operationTrace.setStartTime(System.nanoTime());

    logger.info(
        "Operation ID :{} ; SQL : {}", operationTrace.getOperationID(), operationTrace.getSql());

    //            pStat.setQueryTimeout(1);
    ResultSet resultSet = pStat.executeQuery();

    // 用来储存推算出的pkId
    int pkId;

    // 打印select结果
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int columnsNumber = resultSetMetaData.getColumnCount();
    while (resultSet.next()) {

      // tupleTrace的一部分
      Map<String, String> valueMap = new HashMap<>();
      Map<String, String> realValueMap = new HashMap<>();
      // 是否属于静态区

      pkId = resultSet.getInt(1);

      for (int colInd = 2; colInd <= columnsNumber; colInd++) {

        // 要 -1 , 1-based -> 0-based
        ParamInfo colInfo = selectInfoList.get(colInd - 2);

        // 按照类型读取
        Object columnValue;

        if (colInfo.type == ParamType.UK) {
          columnValue =
              LoaderUtils.readResultSetByType(resultSet, colInd, colInfo.uk.get(0).getAttrType());
        } else {
          columnValue =
              LoaderUtils.readResultSetByType(resultSet, colInd, colInfo.attr.getAttrType());
        }

        //                if (colInfo.type == ParamType.PK) {
        //                    // 存储推算的逻辑值
        //                    pkId = loader.getDbMirror()
        //                            .getLogicalValue(colInfo.table.getTableId(),
        // colInfo.attr.getAttrName(), columnValue.toString())
        //                            .getValue();
        //                }

        // tupleTrace的一部分
        // trace
        valueMap.put(resultSetMetaData.getColumnLabel(colInd), columnValue.toString());
      }

      // tupleTrace
      if (operation.getTable() instanceof Table) {
        // 移除主键属性
        // valueMap.keySet().removeIf(key -> key.startsWith(Symbol.PK_ATTR_PREFIX));

        TupleTrace readTupleTrace =
            new TupleTrace(
                String.valueOf(operation.getTable().getTableName()),
                String.valueOf(pkId),
                valueMap,
                realValueMap);
        readTupleTraceList.add(readTupleTrace);
      } // view 的解析在改版后不一定支持
      //            else {
      //                View fromView = (View) operation.getTable();
      //                Map<Integer, Map<String, String>> valueMapOfTables = new HashMap<>();
      //                Map<Integer, String> pkIdOfMaps = new HashMap<>();
      //                for (String viewColName : valueMap.keySet()) {
      //                    // 需要对View属性名进行改写
      //                    List<ParamInfo> paramInfoList = fromView.findAllInfo(viewColName);
      //                    for (ParamInfo paramInfo : paramInfoList) {
      //                        int fromTableId = paramInfo.table.getTableId();
      //
      //                        // 若不存在则初始化
      //                        valueMapOfTables.computeIfAbsent(fromTableId, k -> new HashMap<>());
      //
      //                        if (paramInfo.type == ParamType.PK) {
      //                            pkIdOfMaps.put(fromTableId, valueMap.get(viewColName));
      //                        } else {
      //
      // valueMapOfTables.get(fromTableId).put(paramInfo.attr.getAttrName(),
      // valueMap.get(viewColName));
      //                        }
      //                    }
      //                }
      //
      //                for (Integer tableId : valueMapOfTables.keySet()) {
      //                    TupleTrace readTupleTrace = new TupleTrace(
      //                            String.valueOf(tableId),
      //                            pkIdOfMaps.get(tableId),
      //                            valueMapOfTables.get(tableId),
      //                            realValueMap);
      //                    readTupleTraceList.add(readTupleTrace);
      //                }
      //            }
    }

    // operationTraceal
    operationTrace.setReadTupleList(readTupleTraceList);
  }
}
