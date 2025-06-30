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
import gen.schema.table.ForeignKey;
import gen.schema.table.Index;
import gen.schema.table.Table;
import gen.schema.view.View;
import io.IOUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.alg.util.Pair;
import symbol.Symbol;
import trace.ReadMode;
import trace.TraceLockMode;
import util.log.ExceptionLogger;

public class AdapterMySQL extends Adapter {

  private static final Logger logger = LogManager.getLogger(AdapterMySQL.class);

  private final DataFormat dataFormat;

  /**
   * 如果需要用到涉及 dataFormat 的方法，那么必须设置，否则可以设为 null
   *
   * @param dataFormat dataFormat对象
   */
  public AdapterMySQL(DataFormat dataFormat) {
    this.dataFormat = dataFormat;
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
        return String.format(
            "double(%d, %d)", dataFormat.decimalPrecision, dataFormat.decimalScale);
      case BLOB:
        if (dataFormat.blobLength <= 255) {
          return "tinyblob";
        } else if (dataFormat.blobLength <= 65535) {
          return "blob";
        } else if (dataFormat.blobLength <= 16777215) {
          return "mediumblob";
        } else {
          return "longblob";
        }
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
    String index_template = "alter table %s add index %s(%s);";
    List<String> columns = new ArrayList<>();
    for (Attribute attribute : attributeList) {
      columns.add(attribute.getAttrName());
    }

    return String.format(index_template, tableName, indexName, String.join(", ", columns));
  }

  /**
   * 创建视图
   *
   * @param view 视图
   * @return 视图创建SQL语句
   */
  @Override
  public String createView(View view) {
    String view_template = "create view %s(%s) as select %s from %s where %s;";
    List<String> attrList = new ArrayList<>();
    List<String> fieldList = new ArrayList<>();
    String attrStr;
    String fieldStr;

    // 2.形成视图中的所有属性信息
    // 2.1形成视图中主键的相关信息
    for (Attribute attribute : view.getPrimaryKey()) {
      attrList.add(attribute.getAttrName());
      ParamInfo paramInfo = view.findParamInfo(attribute);
      assert paramInfo.attr != null;
      String selectName =
          String.format("%s.%s", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
      fieldList.add(selectName);
    }
    // 2.2形成视图中普通外键的相关信息
    for (ForeignKey foreignKey : view.getCommonForeignKeyList()) {
      for (Attribute attribute : foreignKey) {
        attrList.add(attribute.getAttrName());
        ParamInfo paramInfo = view.findParamInfo(attribute);
        assert paramInfo.attr != null;
        String selectName =
            String.format("%s.%s", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
        fieldList.add(selectName);
      }
    }
    // 2.3形成视图中非键值属性的相关信息
    for (Attribute attribute : view.getAttributeList()) {
      attrList.add(attribute.getAttrName());
      ParamInfo paramInfo = view.findParamInfo(attribute);
      assert paramInfo.attr != null;
      String selectName =
          String.format("%s.%s", paramInfo.table.getTableName(), paramInfo.attr.getAttrName());
      fieldList.add(selectName);
    }

    // 生成 attribute 部分 SQL
    attrStr = String.join(", ", attrList);
    fieldStr = String.join(", ", fieldList);

    // 生成join条件
    List<String> joinTables = new ArrayList<>();
    for (Table table : view.getJoin().getJoinTables()) {
      joinTables.add(String.format("%s", table.getTableName()));
    }

    // 生成where条件
    List<String> whereConditions = new ArrayList<>();
    for (Pair<Pair<String, String>, Pair<String, String>> cond :
        view.getJoin().getJoinConditions()) {
      whereConditions.add(
          String.format(
              "%s.%s=%s.%s",
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

  @Override
  public void sessionConfig(Connection conn) throws SQLException {
    @Cleanup Statement stat = conn.createStatement();
    // 允许在自增列插入 0，属性名使用引号包围
    stat.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';");
  }

  @Override
  public TraceLockMode calcTraceLockMode(int isolation, OperationLockMode operationLockMode) {
    if (isolation == Connection.TRANSACTION_READ_UNCOMMITTED
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT:
          return TraceLockMode.NON_LOCK;
        case SELECT_LOCK_IN_SHARE_MODE:
          return TraceLockMode.SHARE_LOCK;
        case SELECT_FOR_UPDATE:
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
        case SELECT_LOCK_IN_SHARE_MODE:
          return TraceLockMode.SHARE_LOCK;
        case SELECT_FOR_UPDATE:
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
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT_FOR_UPDATE:
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.LOCKING_READ;
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else if (isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT:
          return ReadMode.CONSISTENT_READ;
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT_FOR_UPDATE:
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
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT_FOR_UPDATE:
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
    if (isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT_FOR_UPDATE:
        case SELECT_LOCK_IN_SHARE_MODE:
        case SELECT:
          return true;
        default:
          return false;
      }
    } else if (isolation == Connection.TRANSACTION_REPEATABLE_READ) {
      switch (operationLockMode) {
        case SELECT_FOR_UPDATE:
        case SELECT_LOCK_IN_SHARE_MODE:
          return true;
        default:
          return false;
      }
    }
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
        return operation.getTable().getTableName()
            + Symbol.TABLE_ATTRIBUTE_LINKER
            + "table = '"
            + operation.getTable().getTableName()
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

      assert attribute != null;
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
  public void setTransactionIsolation(Connection conn, int isolationLevel) throws SQLException {
    conn.setTransactionIsolation(isolationLevel);
  }

  @Override
  public void startTransaction(Connection conn, OperationStart operationStart) throws SQLException {
    String cmd;
    switch (operationStart.getStartType()) {
      case StartTransactionReadOnly:
        cmd = "START TRANSACTION READ ONLY;";
        break;
      case StartTransactionWithConsistentSnapshot:
        cmd = "START TRANSACTION WITH CONSISTENT SNAPSHOT;";
        break;
      case StartTransactionReadOnlyWithConsistentSnapshot:
        cmd = "START TRANSACTION READ ONLY,WITH CONSISTENT SNAPSHOT;";
        break;
      case START:
      default:
        cmd = "START TRANSACTION;";
        break;
    }

    Statement stat = conn.createStatement();
    stat.execute(cmd);
    stat.close();
  }

  @Override
  public void startTransactionForRestore(Statement stat) throws SQLException {
    stat.execute("start transaction");
  }

  @Override
  public void dump(String username, String password, String dbname, String dest)
      throws IOException {
    Process process =
        Runtime.getRuntime()
            .exec(String.format("mysqldump -u%s -p%s %s", username, password, dbname));
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String lines = reader.lines().collect(Collectors.joining("\n"));
    IOUtils.writeString(lines, dest, false);
  }

  @Override
  public String getDatabase(String connUrl) {
    String[] words = connUrl.split("\\?")[0].split("/");
    return words[words.length - 1];
  }

  /**
   * 生成 某一个 ForeignKey 的 add constraint 语句
   *
   * @param constraintName 约束名称
   * @param foreignKey 外键对象
   * @param type “prefix” or “common”
   * @return 添加外键约束的sql
   */
  @Override
  protected String composeConstraintString(
      String constraintName, ForeignKey foreignKey, String type) {
    // 保证参数 type 无误
    assert ("prefix".equals(type) || "common".equals(type));

    List<String> foreignAttrList = new ArrayList<>();
    for (Attribute attribute : foreignKey) {
      foreignAttrList.add(String.format("%s", attribute.getAttrName()));
    }

    String referenceTableName = foreignKey.getReferencedTable().getTableName();
    List<String> referenceAttrList = new ArrayList<>();

    if ("prefix".equals(type)) {
      for (Attribute attribute : foreignKey) {
        referenceAttrList.add(String.format("%s", attribute.getAttrName()));
      }
    } else {
      for (Attribute attribute : foreignKey.getReferencedAttrGroup()) {
        referenceAttrList.add(String.format("%s", attribute.getAttrName()));
      }
    }

    return String.format(
        "add constraint %s foreign key(%s) references %s(%s)",
        constraintName,
        String.join(", ", foreignAttrList),
        referenceTableName,
        String.join(", ", referenceAttrList));
  }

  @Override
  public void handleException(SQLException e) {
    if (!e.getMessage().contains("Deadlock found when trying to get lock;")
        && !e.getMessage().contains("Query execution was interrupted")
        && !e.getMessage().contains("Statement cancelled")) {
      ExceptionLogger.error(e);
    }
  }

  @Override
  public SQLFilter getSQLFilter() {
    return new SQLFilter() {
      @Override
      public String filter(String sql) {
        return sql;
      }
    };
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
    // 允许在自增列插入 0
    stat.execute("SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO';");
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

  /**
   * 将属性值转换成insert中的形式，各种DBMS有不同的限制
   *
   * @param attrValue attrValue
   * @return value string
   */
  @Override
  protected String attrVal2SQLStr(AttrValue attrValue) {
    return String.format("\"%s\"", attrValue.value);
  }

  /**
   * 插入全部数据，用于初始化表时
   *
   * @param tableName 表名
   * @param col2values 列名->AttrValue
   */
  @Override
  public String insert(String tableName, HashMap<String, AttrValue> col2values) {
    String insertTemp = "insert into %s (%s) values (%s);";
    List<String> columns = new ArrayList<>();
    List<String> values = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      columns.add(String.format("%s", colName));
      values.add(this.attrVal2SQLStr(col2values.get(colName)));
    }

    return String.format(
        insertTemp, tableName, String.join(",", columns), String.join(",", values));
  }

  @Override
  public String update(String tableName, HashMap<String, AttrValue> col2values) {
    String updateTemp = "update `%s` set %s where %s;";
    List<String> pkEqus = new ArrayList<>();
    List<String> allEqus = new ArrayList<>();

    for (String colName : col2values.keySet()) {
      String value = this.attrVal2SQLStr(col2values.get(colName));
      String equ = String.format("`%s` = %s", colName, value);
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
    String deleteTemp = "delete from `%s` where `pkId`=\"%s\";";

    return String.format(deleteTemp, tableName, pkId);
  }

  @Override
  public String insertIgnore(String tableName, HashMap<String, AttrValue> col2values) {
    String insertTemp = "insert ignore into `%s` (%s) values (%s);";
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
  public Pattern getInsertPattern() {
    return Pattern.compile("^insert into (table\\d+) \\((.+)\\) values \\((.*)\\);$");
  }

  public String getRawSQL(String sql) {
    sql = sql.split(":")[1];
    return sql.substring(1);
  }
}
