package adapter;

import context.OrcaContext;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.basic.OperationStart;
import gen.operation.enums.OperationLockMode;
import gen.operation.enums.OperationType;
import gen.schema.table.*;
import gen.schema.view.View;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Cleanup;
import symbol.Symbol;
import trace.IsolationLevel;
import trace.ReadMode;
import trace.TraceLockMode;
import util.jdbc.DataSourceUtils;
import util.log.ExceptionLogger;
import util.rand.RandUtil;
import util.rand.RandUtils;

/** 所有SQL语句的统一接口，当面对一个新数据库时，只需要编写一个类，实现这个接口，就可以把ACIDTester适配到这个数据库上 */
public abstract class Adapter implements Serializable {
  //////////////////// JDBC URL Filter //////////////////////
  public String filterJDBCUrl(String rawJdbcURL, boolean isContainDB) {
    return isContainDB
        ? rawJdbcURL
        : rawJdbcURL.substring(0, rawJdbcURL.lastIndexOf('/') + 1)
            + rawJdbcURL.substring(rawJdbcURL.indexOf('?'));
  }

  //////////////////// Schema //////////////////////

  /**
   * 用于将该程序中的Generic DataType转换成各种DBMS中的表示形式
   *
   * @param dataType dataType
   * @return dataType的字符串表示
   */
  public abstract String typeConvert(DataType dataType);

  /**
   * 删除数据库
   *
   * @param dbName 数据库名称
   */
  public void dropDatabase(String dbName) throws SQLException {
    String sql = String.format("drop database if exists %s;", dbName);
    @Cleanup Connection conn = DataSourceUtils.getJDBCConnection(false);
    @Cleanup Statement stat = conn.createStatement();
    // 重新创建数据库
    stat.execute(sql);
  }

  /**
   * 创建数据库
   *
   * @param dbName 数据库名称
   */
  public void createDatabase(String dbName) throws SQLException {
    String sql = String.format("create database %s;", dbName);
    @Cleanup Connection conn = DataSourceUtils.getJDBCConnection(false);
    @Cleanup Statement stat = conn.createStatement();
    // 重新创建数据库
    stat.execute(sql);
  }

  /**
   * 创建表
   *
   * @param table 表
   * @return 建表语句
   */
  public String createTable(Table table) {
    //  SQL 模板
    String create_template = "create table %s (%s) %s %s;";
    // 暂存 属性定义 和 主键定义 语句，最后拼装
    List<String> attrDefinitions = new ArrayList<>();

    // 生成各个column的定义和主键定义，存入 attrDefinitions

    // 增加一列 pkId
    attrDefinitions.add(String.format("%s %s", "pkId", typeConvert(DataType.INTEGER)));

    // 创建主键
    PrimaryKey primaryKey = table.getPrimaryKey();
    if (primaryKey.size() == 1) {
      String temp;
      if (primaryKey.isAutoIncrease()) {
        temp = "%s %s auto_increment";
      } else {
        temp = "%s %s";
      }
      attrDefinitions.add(
          String.format(
              temp, primaryKey.get(0).getAttrName(), typeConvert(primaryKey.get(0).getAttrType())));
    } else { // ==1需要增加自增的选项
      for (Attribute attribute : primaryKey) {
        attrDefinitions.add(
            String.format("%s %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
      }
    }

    // 非键值属性，
    List<Attribute> attributeList = table.getAttributeList();
    for (Attribute attribute : attributeList) {
      attrDefinitions.add(
          String.format("%s %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
    }
    // 普通外键，注意前缀外键在主键那里已经添加过了
    for (ForeignKey foreignKey : table.getCommonForeignKeyList()) {
      for (Attribute attribute : foreignKey) {
        attrDefinitions.add(
            String.format("%s %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
      }
    }

    // 主键约束
    if (!primaryKey.isEmpty()) {
      List<String> pkList = new ArrayList<>();
      for (Attribute attribute : primaryKey) {
        pkList.add(attribute.getAttrName());
      }
      attrDefinitions.add(
          String.format(
              "primary key(%s) %s",
              String.join(", ", pkList),
              ((this instanceof AdapterTiDB) ? ("NONCLUSTERED") : (""))));
      //              ((this instanceof AdapterTiDB) ? ("") : (""))));  // TiDB v4.0.7 不支持
      // NONCLUSTERED
    }

    UniqueKey uniqueKey = table.getUniqueKey();
    for (Attribute uk : uniqueKey) {
      attrDefinitions.add(String.format("%s %s", uk.getAttrName(), typeConvert(uk.getAttrType())));
    }

    // uk约束
    if (!OrcaContext.configColl.getSchema().getTable().isUsePartition() && !uniqueKey.isEmpty()) {
      List<String> ukList = new ArrayList<>();
      for (Attribute uk : uniqueKey) {
        ukList.add(uk.getAttrName());
      }
      attrDefinitions.add(String.format("UNIQUE (%s)", String.join(", ", ukList)));
    }

    String partitionRule = "";
    if (table.getPartition() != null) {
      partitionRule = table.getPartition().toString();
    }

    return String.format(
        create_template,
        table.getTableName(),
        String.join(", ", attrDefinitions),
        table.getRowFormat(),
        partitionRule);
  }

  protected String foreign_template = "alter table %s %s;";

  /**
   * 生成追加外键的操作语句
   *
   * @param table table
   * @return list of SQL
   */
  public List<String> createForeignKey(Table table) {
    ForeignKey primaryKey2ForeignKey = table.getPk2ForeignKey();
    List<ForeignKey> commonForeignKeys = table.getCommonForeignKeyList();
    // 如果没有外键，直接返回
    if (primaryKey2ForeignKey == null && commonForeignKeys.isEmpty()) return new ArrayList<>();

    // 表名
    String alterTableName = "table" + table.getTableId();

    // 构造 constraints
    List<String> constraintsList = new ArrayList<>();
    // 2.生成前缀外键
    if (primaryKey2ForeignKey != null) {
      String constraintName =
          String.format(
              "%s_%s_%s%s",
              Symbol.PF_FK,
              table.getTableId(),
              Symbol.FK_ATTR_PREFIX,
              primaryKey2ForeignKey.getId());
      constraintsList.add(composeConstraintString(constraintName, primaryKey2ForeignKey, "prefix"));
    }
    // 3.普通外键约束
    for (ForeignKey foreignKey : commonForeignKeys) {
      String constraintName =
          String.format(
              "%s_%s_%s%s",
              Symbol.COMM_FK, table.getTableId(), Symbol.FK_ATTR_PREFIX, foreignKey.getId());
      constraintsList.add(composeConstraintString(constraintName, foreignKey, "common"));
    }

    ArrayList<String> alterConstraintSqlList = new ArrayList<>();
    for (String constraintStr : constraintsList) {
      alterConstraintSqlList.add(String.format(foreign_template, alterTableName, constraintStr));
    }

    return alterConstraintSqlList;
  }

  public abstract String createIndex(String name, String tableName, List<Attribute> attributeList);

  /**
   * 创建视图
   *
   * @param view 视图
   * @return 视图创建SQL语句
   */
  public abstract String createView(View view);

  //////////////  Session Config  /////////////
  public abstract void sessionConfig(Connection conn) throws SQLException;

  /////////////// Data//////////////////////
  public abstract void loadCSV(Connection conn, String tableName, String columns, String csvSrc)
      throws SQLException;

  public abstract void loadInsertList(Connection conn, List<String> sqlList) throws SQLException;

  /**
   * 将属性值转换成insert中的形式，各种DBMS有不同的限制
   *
   * @param attrValue attrValue
   * @return value string
   */
  protected abstract String attrVal2SQLStr(AttrValue attrValue);

  ////////////// Transaction////////////////

  /**
   * 插入全部数据，用于初始化表时
   *
   * @param tableName 表名
   * @param col2values 列名->AttrValue
   */
  public abstract String insert(String tableName, HashMap<String, AttrValue> col2values);

  public abstract String update(String tableName, HashMap<String, AttrValue> col2values);

  public abstract String delete(String tableName, int pkId);

  public abstract String insertIgnore(String tableName, HashMap<String, AttrValue> col2values);

  public abstract Pattern getInsertPattern();

  /**
   * 生成一个该数据库系统所支持的隔离级别
   *
   * @return isolation
   */
  public int randIsolation(Map<IsolationLevel, Integer> isolationMap) {
    if (isolationMap == null || isolationMap.isEmpty()) {
      throw new RuntimeException("隔离级别不能为空");
    }
    IsolationLevel isolationLevel = RandUtils.randSelectByProbability(isolationMap);
    return IsolationLevel.enum2IntIsolation(isolationLevel);
  }

  /**
   * 使用给定的随机工具生成一个该数据库系统所支持的隔离级别
   *
   * @return isolation
   */
  public int randIsolation(Map<IsolationLevel, Integer> isolationMap, RandUtil randUtil) {
    if (isolationMap == null || isolationMap.isEmpty()) {
      throw new RuntimeException("隔离级别不能为空");
    }
    IsolationLevel isolationLevel = randUtil.randSelectByProbability(isolationMap);
    return IsolationLevel.enum2IntIsolation(isolationLevel);
  }

  public abstract TraceLockMode calcTraceLockMode(
      int isolation, OperationLockMode operationLockMode);

  public abstract ReadMode calcTraceReadMode(int isolation, OperationLockMode operationLockMode);

  public abstract String writePredicate(
      int isolation, Operation operation, List<AttrValue> attrValues);

  /**
   * 设置事务的隔离级别
   *
   * @param conn 数据库连接
   * @param isolation 隔离级别
   * @throws SQLException SQLException
   */
  public abstract void setTransactionIsolation(Connection conn, int isolation) throws SQLException;

  /**
   * 执行 START 操作
   *
   * @param conn conn
   * @param operationStart operationStart
   */
  public abstract void startTransaction(Connection conn, OperationStart operationStart)
      throws SQLException;

  public abstract void startTransactionForRestore(Statement stat) throws SQLException;

  //////////////// Utils /////////////////

  public abstract void dump(String username, String password, String dbname, String dest)
      throws IOException;

  public abstract String getDatabase(String connUrl);

  /**
   * 生成 某一个 ForeignKey 的 add constraint 语句
   *
   * @param constraintName 约束名称
   * @param foreignKey 外键对象
   * @param type “prefix” or “common”
   * @return ForeignKey 的 add constraint 语句
   */
  protected abstract String composeConstraintString(
      String constraintName, ForeignKey foreignKey, String type);

  ////////////////// ERROR /////////////////////

  public void handleException(SQLException e) {
    ExceptionLogger.error(e);
  }

  ////////////////// OPERATION ///////////////////////

  /**
   * 将 OperationLockMode 转为 字符串
   *
   * @param operationLockMode operationLockMode
   * @return 用于构造SQL的字符串
   */
  public String lockMode(OperationLockMode operationLockMode) {
    switch (operationLockMode) {
      case SELECT_FOR_UPDATE:
        return "for update";
      case SELECT_LOCK_IN_SHARE_MODE:
        return "lock in share mode";
      default:
        return "";
    }
  }

  ///////////////// SQLFilter /////////////////

  /**
   * 获取SQLFilter，SQLFilter用于改写事务模板，以适配特定DBMS
   *
   * @return sqlFilter
   */
  public abstract SQLFilter getSQLFilter();

  //////////////// SavePoint /////////////////
  public Savepoint setSavepoint(Connection conn, String savepointName) throws SQLException {
    return conn.setSavepoint(savepointName);
  }

  public String getRawSQL(String sql) {
    return sql;
  }

  public boolean isSnapshotPoint(
      IsolationLevel isolationLevel,
      OperationType opType,
      boolean isFirstReadOp,
      boolean isFirstWriteOp) {
    return opType == OperationType.START;
  }

  public boolean isCurrentRead(IsolationLevel isolationLevel, OperationType nowOpType) {
    return nowOpType == OperationType.UPDATE
        || nowOpType == OperationType.DELETE
        || nowOpType == OperationType.INSERT;
  }

  public void copyTable(String fromDB, String fromTable, String toDB, String toTable)
      throws SQLException {
    String copySQL =
        String.format("INSERT INTO %s.%s SELECT * FROM %s.%s;", toDB, toTable, fromDB, fromTable);
    @Cleanup Connection conn = DataSourceUtils.getJDBCConnection(true);
    @Cleanup Statement stat = conn.createStatement();
    // 导入数据
    stat.execute(copySQL);
  }

  public void copyTableSchema(String fromDB, String fromTable, String toDB, String toTable)
      throws SQLException {
    String copySQL =
        String.format("CREATE TABLE %s.%s LIKE %s.%s;", toDB, toTable, fromDB, fromTable);
    @Cleanup Connection conn = DataSourceUtils.getJDBCConnection(true);
    @Cleanup Statement stat = conn.createStatement();
    // 导入数据
    stat.execute(copySQL);
  }
}
