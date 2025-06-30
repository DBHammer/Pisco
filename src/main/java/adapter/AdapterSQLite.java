package adapter;

import gen.data.format.DataFormat;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.basic.OperationStart;
import gen.operation.basic.where.PredicateLock;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.operation.param.ParamInfo;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.Index;
import gen.schema.table.Table;
import gen.schema.view.View;
import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.Cleanup;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import symbol.Symbol;
import trace.ReadMode;
import trace.TraceLockMode;
import util.log.ExceptionLogger;

public class AdapterSQLite extends AdapterOceanBase {

  private static final Logger logger = LogManager.getLogger(AdapterSQLite.class);

  /** 如果需要用到涉及 dataFormat 的方法，那么必须设置，否则可以设为 null */
  public AdapterSQLite(DataFormat dataFormat) {
    super(dataFormat);
  }

  public String typeConvert(DataType dataType) {
    switch (dataType) {
      case INTEGER:
        return "integer";
      case VARCHAR:
        return "text";
      case DECIMAL:
        return "numeric";
      case DOUBLE:
        return "real";
      case BLOB:
        return "blob";
      default:
        throw new NotImplementedException();
    }
  }

  /**
   * 创建索引的SQL, 包含两个元素，建立索引的表和建立索引的列和索引所在列的类型,服务于建表时建立索引
   *
   * @param indexName indexName
   * @param tableName tableName
   * @param attributeList attributeList
   * @return SQL of create index
   */
  public String createIndex(String indexName, String tableName, List<Attribute> attributeList) {
    String index_template = "create index %s on %s(%s);";
    List<String> columns = new ArrayList<>();
    for (Attribute attribute : attributeList) {
      columns.add(attribute.getAttrName());
    }

    return String.format(index_template, indexName, tableName, String.join(", ", columns));
  }

  @Override
  public void sessionConfig(Connection conn) {}

  @Override
  public TraceLockMode calcTraceLockMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.NON_LOCK;
        case INSERT:
        case UPDATE:
        case DELETE:
          return TraceLockMode.EXCLUSIVE_LOCK;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else if (isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.SHARE_LOCK;
        case INSERT:
        case UPDATE:
        case DELETE:
          return TraceLockMode.EXCLUSIVE_LOCK;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  @Override
  public ReadMode calcTraceReadMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED) {
      switch (operationLockMode) {
        case SELECT:
          return ReadMode.UNCOMMITTED_READ;
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.LOCKING_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else if (isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT:
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.LOCKING_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  /**
   * 判断是否需要添加
   *
   * @param isolation isolation
   * @param operationLockMode operationLockMode
   * @return 是否需要重写Predicate
   */
  private boolean needRewritePredicate(int isolation, OperationLockMode operationLockMode) {
    return false;
  }

  @Override
  public String writePredicate(int isolation, Operation operation, List<AttrValue> attrValues) {

    // 不需要记录时返回直接结束
    if (!needRewritePredicate(isolation, operation.getOperationLockMode())) return null;
    AbstractTable abTable = operation.getTable();
    // 如果没有where clause返回所有的表
    if (operation.getWhereClause().getBaseAttributeList().isEmpty()) {
      if (abTable instanceof Table) {
        return abTable.getTableName()
            + Symbol.TABLE_ATTRIBUTE_LINKER
            + "table = '"
            + abTable.getTableName()
            + "'";
      } else {
        View view = (View) abTable;
        List<String> ret = new ArrayList<>();
        for (Table table : view.getJoin().getJoinTables()) {
          ret.add(
              table.getTableName()
                  + Symbol.TABLE_ATTRIBUTE_LINKER
                  + "table = '"
                  + table.getTableName()
                  + "'");
        }
        return String.join(" or ", ret);
      }
    }

    // 去掉前面投影的参数
    List<String> ret = new ArrayList<>();
    int num = 0;
    if (operation.getProject() != null && operation.getOperationType() != OperationType.SELECT) {
      num += operation.getProject().getBaseAttributeList().size();
      if (operation.getOperationType() == OperationType.INSERT) num++;
    }

    List<PredicateLock> predicateLocks = operation.getWhereClause().getPredicates();

    // 检查是否有索引，因为唯一包含view的select一定包含主键，所以不用检查view
    if (operation.getTable() instanceof Table) {
      Table table = (Table) abTable;
      Index index = table.getIndex();
      boolean hasIndex = false;
      for (PredicateLock predicateLock : predicateLocks) {
        if (index.isInIndex(predicateLock.getLeftValue())) {
          hasIndex = true;
        }
      }
      if (!hasIndex) {
        return operation.getTable()
            + Symbol.TABLE_ATTRIBUTE_LINKER
            + "table = '"
            + operation.getTable()
            + "'";
      }
    }

    // 开始扫描predicate
    for (int idx = 0; idx < predicateLocks.size(); ++idx) {
      PredicateLock predicateLock = predicateLocks.get(idx);
      List<String> tmp = new ArrayList<>();
      Attribute attribute = predicateLock.getLeftValue();
      // 从真实值的列表里取出对应的真实值
      for (int i = 0; i < predicateLock.getParameterNumber(); ++i) {
        tmp.add('\'' + attrValues.get(num).value.toString() + '\'');
        num++;
      }

      AbstractTable tmpTable = abTable;
      if (abTable instanceof View) {
        View view = (View) abTable;
        ParamInfo paramInfo = view.findParamInfo(attribute);
        tmpTable = paramInfo.table;
        attribute = paramInfo.attr;
      }

      // 如果不是like或者in，不需要修改，直接替换真实值生成语句
      switch (predicateLock.getPredicateOperator()) {
        case IN:
          if (predicateLock.getIsNot()) {
            ret.add(
                tmpTable.getTableName()
                    + Symbol.TABLE_ATTRIBUTE_LINKER
                    + "table = '"
                    + tmpTable.getTableName()
                    + "'");
          } else {
            ret.add(
                predicateLock.toString(
                    tmpTable.getTableName()
                        + Symbol.TABLE_ATTRIBUTE_LINKER
                        + attribute.getAttrName(),
                    tmp));
          }
          break;
        case LIKE:
          ret.add(
              tmpTable.getTableName()
                  + Symbol.TABLE_ATTRIBUTE_LINKER
                  + "table = '"
                  + tmpTable.getTableName()
                  + "'");
          break;
        case ISNULL:
          ret.add(
              "isnull "
                  + tmpTable.getTableName()
                  + Symbol.TABLE_ATTRIBUTE_LINKER
                  + attribute.getAttrName());
          break;
        default:
          ret.add(
              predicateLock.toString(
                  tmpTable.getTableName() + Symbol.TABLE_ATTRIBUTE_LINKER + attribute.getAttrName(),
                  tmp));
      }

      if (idx < predicateLocks.size() - 1) {
        ret.add(operation.getWhereClause().getConnect().get(idx));
      }
    }

    return String.join(" ", ret); // operation.getWhereClause().toSQL();
  }

  @Override
  public void setTransactionIsolation(Connection conn, int isolation) throws SQLException {
    //        Statement statement = conn.createStatement();
    //
    //        // command
    //        String command;
    //        switch (isolation) {
    //            case Connection.TRANSACTION_SERIALIZABLE:
    //                command = "PRAGMA read_uncommitted = false";
    //                break;
    //            case  Connection.TRANSACTION_READ_UNCOMMITTED:
    //                command = "PRAGMA read_uncommitted = true";
    //                break;
    //            default:
    //                throw new RuntimeException("不支持的隔离级别");
    //        }
    //
    //        // 设置隔离级别
    //        statement.execute(command);
    //        // 关闭statement
    //        statement.close();
    conn.setTransactionIsolation(isolation);
  }

  @Override
  public void startTransaction(Connection conn, OperationStart operationStart) {
    //        String cmd;
    //        switch (operationStart.getStartType()) {
    //            case StartTransactionReadOnly:
    //                cmd = "START TRANSACTION READ ONLY;";
    //                break;
    //            case StartTransactionWithConsistentSnapshot:
    //                cmd = "START TRANSACTION WITH CONSISTENT SNAPSHOT;";
    //                break;
    //            case StartTransactionReadOnlyWithConsistentSnapshot:
    //                cmd = "START TRANSACTION READ ONLY,WITH CONSISTENT SNAPSHOT;";
    //                break;
    //            case START:
    //            default:
    //                cmd = " begin TRANSACTION;";
    //                break;
    //        }
    //
    //        Statement stat = conn.createStatement();
    //        stat.execute(cmd);
    //        stat.close();
  }

  @Override
  public void startTransactionForRestore(Statement stat) {
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      logger.warn(e);
    }
  }

  @Override
  public void dump(String username, String password, String dbname, String dest) {
    throw new NotImplementedException();
  }

  @Override
  public String getDatabase(String connUrl) {
    throw new NotImplementedException();
  }

  @Override
  public void handleException(SQLException e) {
    if (!e.getMessage().startsWith("Deadlock found when trying to get lock;")
        && !e.getMessage().startsWith("Query execution was interrupted")) {
      ExceptionLogger.error(e);
    }
  }

  @Override
  public void loadCSV(Connection conn, String tableName, String columns, String csvSrc)
      throws SQLException {
    @Cleanup Statement stat = conn.createStatement();
    // 允许在自增列插入 0
    stat.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';");
    String sql =
        String.format(
            "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s "
                + "FIELDS TERMINATED BY ',' ENCLOSED BY '\"' "
                +
                //                        "LINES TERMINATED BY '\\n' " +
                "IGNORE 1 LINES (%s);",
            csvSrc, tableName, columns);
    logger.info(sql);
    stat.execute(sql);
  }

  @Override
  public void loadInsertList(Connection conn, List<String> sqlList) throws SQLException {
    @Cleanup Statement stat = conn.createStatement();
    //        // 允许在自增列插入 0
    //        stat.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';");
    // 导入 schema
    for (String sql : sqlList) {
      try {
        stat.execute(sql);
      } catch (SQLException e) {
        logger.error(String.format("Failed to execute: %s", sql));
        throw e;
      }
    }
  }

  @Override
  public String insertIgnore(String tableName, HashMap<String, AttrValue> col2values) {
    String insertTemp = "insert or ignore into `%s` (%s) values (%s);";
    List<String> columns = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      columns.add(String.format("`%s`", colName));
      values.add(this.attrVal2SQLStr(col2values.get(colName)));
    }

    return String.format(
        insertTemp, tableName, String.join(",", columns), String.join(",", values));
  }

  @Override
  public void dropDatabase(String dbName) {
    String dbFilename = dbName + ".db";
    File dbFile = new File(dbFilename);
    dbFile.delete();
  }

  @Override
  public String filterJDBCUrl(String rawJdbcURL, boolean isContainDB) {
    return rawJdbcURL;
  }

  @Override
  public Savepoint setSavepoint(Connection conn, String savepointName) throws SQLException {
    return conn.setSavepoint();
  }
}
