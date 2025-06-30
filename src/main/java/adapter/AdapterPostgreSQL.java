package adapter;

import gen.data.format.DataFormat;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.basic.OperationStart;
import gen.operation.enums.OperationLockMode;
import gen.operation.param.ParamInfo;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.PrimaryKey;
import gen.schema.table.Table;
import gen.schema.view.View;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.alg.util.Pair;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import symbol.Symbol;
import trace.ReadMode;
import trace.TraceLockMode;

public class AdapterPostgreSQL extends Adapter {

  private static final Logger logger = LogManager.getLogger(AdapterPostgreSQL.class);

  private final DataFormat dataFormat;

  public AdapterPostgreSQL(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
    this.foreign_template = "alter table \"%s\" %s;";
  }

  public String typeConvert(DataType dataType) {
    switch (dataType) {
      case INTEGER:
        return "integer";
      case VARCHAR:
        return String.format("varchar(%d)", dataFormat.varcharLength);
      case DECIMAL:
        return String.format(
            "decimal(%d, %d)", dataFormat.decimalPrecision, dataFormat.decimalScale);
      case DOUBLE:
        return "double precision";
      case BLOB:
        return "bytea";
      default:
        throw new NotImplementedException();
    }
  }

  /**
   * 创建表
   *
   * @param table 表
   * @return 建表语句
   */
  @Override
  public String createTable(Table table) {
    //  SQL 模板
    String create_template = "create table \"%s\" (%s);";
    // 暂存 属性定义 和 主键定义 语句，最后拼装
    List<String> attrDefinitions = new ArrayList<>();

    // 生成各个column的定义和主键定义，存入 attrDefinitions

    // 增加一列 pkId
    attrDefinitions.add(String.format("\"%s\" %s", "pkId", typeConvert(DataType.INTEGER)));

    // 创建主键
    PrimaryKey primaryKey = table.getPrimaryKey();
    if (primaryKey.size() == 1) { // ==1需要增加自增的选项
      String temp = "\"%s\" %s";
      String type;
      if (primaryKey.isAutoIncrease()) {
        type = "serial";
      } else {
        type = typeConvert(primaryKey.get(0).getAttrType());
      }
      attrDefinitions.add(String.format(temp, primaryKey.get(0).getAttrName(), type));
    } else {
      for (Attribute attribute : primaryKey) {
        attrDefinitions.add(
            String.format(
                "\"%s\" %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
      }
    }

    // 非键值属性，
    List<Attribute> attributeList = table.getAttributeList();
    for (Attribute attribute : attributeList) {
      attrDefinitions.add(
          String.format(
              "\"%s\" %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
    }
    // 普通外键，注意前缀外键在主键那里已经添加过了
    for (ForeignKey foreignKey : table.getCommonForeignKeyList()) {
      for (Attribute attribute : foreignKey) {
        attrDefinitions.add(
            String.format(
                "\"%s\" %s", attribute.getAttrName(), typeConvert(attribute.getAttrType())));
      }
    }

    // 主键约束
    if (!primaryKey.isEmpty()) {
      List<String> pkList = new ArrayList<>();
      for (Attribute attribute : primaryKey) {
        pkList.add(String.format("\"%s\"", attribute.getAttrName()));
      }
      attrDefinitions.add(String.format("primary key(%s)", String.join(", ", pkList)));
    }

    return String.format(create_template, table.getTableName(), String.join(", ", attrDefinitions));
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
    String index_template = "create index \"%s\" on \"%s\"(%s);";
    List<String> columns = new ArrayList<>();
    for (Attribute attribute : attributeList) {
      columns.add(String.format("\"%s\"", attribute.getAttrName()));
    }

    return String.format(
        index_template,
        String.format("%s_%s", tableName, indexName),
        tableName,
        String.join(", ", columns));
  }

  @Override
  public void sessionConfig(Connection conn) {}

  @SneakyThrows
  @Override
  public void loadCSV(Connection conn, String tableName, String columns, String csvSrc) {
    @Cleanup FileInputStream fin = new FileInputStream(csvSrc);
    CopyManager copyManager = new CopyManager((BaseConnection) conn);
    String cmd =
        String.format("copy %s from stdin with delimiter ',' quote '''' csv header", tableName);

    logger.info(cmd);

    copyManager.copyIn(cmd, fin);
  }

  @Override
  public void loadInsertList(Connection conn, List<String> sqlList) throws SQLException {
    @Cleanup Statement stat = conn.createStatement();
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
  public TraceLockMode calcTraceLockMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ
        || isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.NON_LOCK;
        case SELECT_LOCK_IN_SHARE_MODE:
          return TraceLockMode.SHARE_LOCK;
        case INSERT:
        case UPDATE:
        case DELETE:
        case SELECT_FOR_UPDATE:
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
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ
        || isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case INSERT:
          return ReadMode.LOCKING_READ;
        case SELECT:
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT_FOR_UPDATE:
        case UPDATE:
        case DELETE:
          return ReadMode.CONSISTENT_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  @Override
  public String writePredicate(int isolation, Operation operation, List<AttrValue> attrValues) {
    return null;
  }

  @Override
  public void setTransactionIsolation(Connection conn, int isolationLevel) throws SQLException {
    conn.setTransactionIsolation(isolationLevel);
  }

  @Override
  public void startTransaction(Connection conn, OperationStart operationStart) throws SQLException {
    Statement stat = conn.createStatement();

    // 设置 读写/只读 & 快照模式
    switch (operationStart.getStartType()) {
      case StartTransactionReadOnly:
        stat.execute("SET TRANSACTION READ ONLY;");
        break;
      case StartTransactionReadOnlyWithConsistentSnapshot:
        stat.execute("SET TRANSACTION READ ONLY;");
        //                stat.execute(String.format("SET TRANSACTION SNAPSHOT '%s';", snapshotId));
        break;
      case StartTransactionWithConsistentSnapshot:
        stat.execute("SET TRANSACTION READ WRITE;");
        //                stat.execute(String.format("SET TRANSACTION SNAPSHOT '%s';", snapshotId));
        break;
      case START:
      default:
        stat.execute("SET TRANSACTION READ WRITE;");
        break;
    }
    stat.close();
  }

  @Override
  public void startTransactionForRestore(Statement stat) throws SQLException {
    stat.execute("start transaction");
  }

  @Override
  public void dump(String username, String password, String dbname, String dest) {
    throw new NotImplementedException();
  }

  @Override
  public String getDatabase(String connUrl) {
    throw new NotImplementedException();
  }

  /**
   * 生成 某一个 ForeignKey 的 add constraint 语句
   *
   * @param constraintName 约束名称
   * @param foreignKey 外键对象
   * @param type “prefix” or “common”
   * @return ForeignKey 的 add constraint 语句
   */
  @Override
  protected String composeConstraintString(
      String constraintName, ForeignKey foreignKey, String type) {
    // 保证参数 type 无误
    assert ("prefix".equals(type) || "common".equals(type));

    List<String> foreignAttrList = new ArrayList<>();
    for (Attribute attribute : foreignKey) {
      foreignAttrList.add(String.format("\"%s\"", attribute.getAttrName()));
    }

    String referenceTableName = foreignKey.getReferencedTable().getTableName();
    List<String> referenceAttrList = new ArrayList<>();

    if ("prefix".equals(type)) {
      for (Attribute attribute : foreignKey) {
        referenceAttrList.add(String.format("\"%s\"", attribute.getAttrName()));
      }
    } else {
      for (Attribute attribute : foreignKey.getReferencedAttrGroup()) {
        referenceAttrList.add(String.format("\"%s\"", attribute.getAttrName()));
      }
    }

    return String.format(
        "add constraint \"%s\" foreign key(%s) references \"%s\"(%s)",
        constraintName,
        String.join(", ", foreignAttrList),
        referenceTableName,
        String.join(", ", referenceAttrList));
  }

  /**
   * 将 OperationLockMode 转为 字符串
   *
   * @param operationLockMode operationLockMode
   * @return 用于构造SQL的字符串
   */
  @Override
  public String lockMode(OperationLockMode operationLockMode) {
    switch (operationLockMode) {
      case SELECT_FOR_UPDATE:
        return "for update";
      case SELECT_LOCK_IN_SHARE_MODE:
        return "for share";
      default:
        return "";
    }
  }

  @Override
  public SQLFilter getSQLFilter() {
    return new SQLFilter() {
      @Override
      public String filter(String sql) {
        if (sql == null) return null;
        return sql.replace("`", "\"");
      }
    };
  }

  /**
   * 将属性值转换成insert中的形式，各种DBMS有不同的限制
   *
   * @param attrValue attrValue
   * @return value string
   */
  @Override
  protected String attrVal2SQLStr(AttrValue attrValue) {
    return String.format("'%s'", attrValue.value);
  }

  /**
   * 插入全部数据，用于初始化表时
   *
   * @param tableName 表名
   * @param col2values 列名->AttrValue
   */
  @Override
  public String insert(String tableName, HashMap<String, AttrValue> col2values) {
    String insertTemp = "insert into \"%s\" (%s) values (%s);";
    List<String> columns = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      columns.add(String.format("\"%s\"", colName));
      values.add(this.attrVal2SQLStr(col2values.get(colName)));
    }

    return String.format(
        insertTemp, tableName, String.join(",", columns), String.join(",", values));
  }

  @Override
  public String update(String tableName, HashMap<String, AttrValue> col2values) {
    String updateTemp = "update \"%s\" set %s where %s;";
    List<String> pkEqus = new ArrayList<>();
    List<String> allEqus = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      String value = this.attrVal2SQLStr(col2values.get(colName));
      String equ = String.format("\"%s\" = %s", colName, value);
      allEqus.add(equ);
      if (colName.startsWith(Symbol.PK_ATTR_PREFIX)) {
        pkEqus.add(equ);
      }
    }

    return String.format(
        updateTemp, tableName, String.join(", ", allEqus), String.join(" and ", pkEqus));
  }

  @Override
  public String delete(String tableName, int pkId) {
    String deleteTemp = "delete from \"%s\" where \"pkId\"='%s';";

    return String.format(deleteTemp, tableName, pkId);
  }

  @Override
  public String insertIgnore(String tableName, HashMap<String, AttrValue> col2values) {
    String insertTemp = "insert into \"%s\" (%s) values (%s) ON CONFLICT DO NOTHING;";
    List<String> columns = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      columns.add(String.format("\"%s\"", colName));
      values.add(this.attrVal2SQLStr(col2values.get(colName)));
    }

    return String.format(
        insertTemp,
        tableName,
        String.join(",", columns),
        String.join(",", values),
        col2values.get("pkId").value.toString());
  }

  @Override
  public Pattern getInsertPattern() {
    return Pattern.compile("^insert into \"(table\\d+)\" \\((.+)\\) values \\((.*)\\);$");
  }

  /**
   * 创建视图
   *
   * @param view 视图
   * @return 视图创建SQL语句
   */
  @Override
  public String createView(View view) {
    String view_template = "create view \"%s\"(%s) as select %s from %s where %s;";
    List<String> attrList = new ArrayList<>();
    List<String> fieldList = new ArrayList<>();
    String attrStr;
    String fieldStr;

    // 2.形成视图中的所有属性信息
    // 2.1形成视图中主键的相关信息
    for (Attribute attribute : view.getPrimaryKey()) {
      attrList.add(String.format("\"%s\"", attribute.getAttrName()));
      ParamInfo paramInfo = view.findParamInfo(attribute);
      String selectName =
          String.format(
              "\"%s\".\"%s\"", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
      fieldList.add(selectName);
    }
    // 2.2形成视图中普通外键的相关信息
    for (ForeignKey foreignKey : view.getCommonForeignKeyList()) {
      for (Attribute attribute : foreignKey) {
        attrList.add(String.format("\"%s\"", attribute.getAttrName()));
        ParamInfo paramInfo = view.findParamInfo(attribute);
        String selectName =
            String.format(
                "\"%s\".\"%s\"", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
        fieldList.add(selectName);
      }
    }
    // 2.3形成视图中非键值属性的相关信息
    for (Attribute attribute : view.getAttributeList()) {
      attrList.add(String.format("\"%s\"", attribute.getAttrName()));
      ParamInfo paramInfo = view.findParamInfo(attribute);
      String selectName =
          String.format(
              "\"%s\".\"%s\"", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
      fieldList.add(selectName);
    }

    // 生成 attribute 部分 SQL
    attrStr = String.join(", ", attrList);
    fieldStr = String.join(", ", fieldList);

    // 生成join条件
    List<String> joinTables = new ArrayList<>();
    for (Table table : view.getJoin().getJoinTables()) {
      joinTables.add(String.format("\"%s\"", table.getTableName()));
    }

    // 生成where条件
    List<String> whereConditions = new ArrayList<>();
    for (Pair<Pair<String, String>, Pair<String, String>> cond :
        view.getJoin().getJoinConditions()) {
      whereConditions.add(
          String.format(
              "\"%s\".\"%s\"=\"%s\".\"%s\"",
              cond.getFirst().getFirst(),
              cond.getFirst().getSecond(),
              cond.getSecond().getFirst(),
              cond.getSecond().getSecond()));
    }

    return String.format(
        view_template,
        view.getTableName(),
        attrStr,
        fieldStr,
        String.join(", ", joinTables),
        String.join(" and ", whereConditions));
  }
}
