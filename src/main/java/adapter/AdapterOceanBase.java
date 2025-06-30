package adapter;

import gen.data.format.DataFormat;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.Operation;
import gen.operation.enums.OperationLockMode;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.PrimaryKey;
import gen.schema.table.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import trace.ReadMode;
import trace.TraceLockMode;

public class AdapterOceanBase extends AdapterMySQL {
  public AdapterOceanBase(DataFormat dataFormat) {
    super(dataFormat);
  }

  @Override
  public String createTable(Table table) {
    //  SQL 模板
    String create_template = "create table %s (%s);";
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
      attrDefinitions.add(String.format("primary key(%s)", String.join(", ", pkList)));
    }

    // 外键约束
    List<String> fks = createForeignKeyForCreateTable(table);
    attrDefinitions.addAll(fks);

    return String.format(create_template, table.getTableName(), String.join(", ", attrDefinitions));
  }

  protected List<String> createForeignKeyForCreateTable(Table table) {
    ForeignKey primaryKey2ForeignKey = table.getPk2ForeignKey();
    List<ForeignKey> commonForeignKeys = table.getCommonForeignKeyList();
    // 如果没有外键，直接返回
    if (primaryKey2ForeignKey == null && commonForeignKeys.isEmpty()) return new ArrayList<>();

    // 构造 constraints
    List<String> constraintsList = new ArrayList<>();
    // 2.生成前缀外键
    if (primaryKey2ForeignKey != null) {
      constraintsList.add(composeConstraintStringForCreateTable(primaryKey2ForeignKey, "prefix"));
    }
    // 3.普通外键约束
    for (ForeignKey foreignKey : commonForeignKeys) {
      constraintsList.add(composeConstraintStringForCreateTable(foreignKey, "common"));
    }

    return constraintsList;
  }

  /**
   * 生成追加外键的操作语句
   *
   * @param table table
   * @return empty list
   */
  public List<String> createForeignKey(Table table) {
    // 普通用户不能alter ... foreign key
    return new ArrayList<>();
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
        case INSERT:
        case UPDATE:
        case DELETE:
        case SELECT_FOR_UPDATE:
          return TraceLockMode.EXCLUSIVE_LOCK;
        case SELECT_LOCK_IN_SHARE_MODE:
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
        case SELECT_FOR_UPDATE:
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.CONSISTENT_READ;
        case SELECT_LOCK_IN_SHARE_MODE:
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else if (isolation == Connection.TRANSACTION_READ_COMMITTED
        || isolation == Connection.TRANSACTION_REPEATABLE_READ
        || isolation == Connection.TRANSACTION_SERIALIZABLE) {
      switch (operationLockMode) {
        case SELECT:
        case SELECT_FOR_UPDATE:
        case INSERT:
        case UPDATE:
        case DELETE:
          return ReadMode.CONSISTENT_READ;
        case SELECT_LOCK_IN_SHARE_MODE:
        default:
          throw new RuntimeException("不支持的 OperationLockMode");
      }
    } else {
      throw new RuntimeException("不支持的隔离级别");
    }
  }

  @Override
  public void setTransactionIsolation(Connection conn, int isolation) throws SQLException {
    Statement statement = conn.createStatement();

    // command
    String level;
    switch (isolation) {
      case Connection.TRANSACTION_READ_COMMITTED:
        level = "READ COMMITTED";
        break;
      case Connection.TRANSACTION_REPEATABLE_READ:
        level = "REPEATABLE READ";
        break;
      case Connection.TRANSACTION_SERIALIZABLE:
        level = "SERIALIZABLE";
        break;
      case Connection.TRANSACTION_READ_UNCOMMITTED:
      default:
        throw new RuntimeException("不支持的隔离级别");
    }

    String command = String.format("SET TRANSACTION ISOLATION LEVEL %s;", level);

    // 设置隔离级别
    statement.execute(command);
    // 关闭statement
    statement.close();
  }

  private String composeConstraintStringForCreateTable(ForeignKey foreignKey, String type) {

    // 保证参数 type 无误
    assert ("prefix".equals(type) || "common".equals(type));

    List<String> foreignAttrList = new ArrayList<>();
    for (Attribute attribute : foreignKey) {
      foreignAttrList.add(attribute.getAttrName());
    }

    String referenceTableName = foreignKey.getReferencedTable().getTableName();
    List<String> referenceAttrList = new ArrayList<>();

    if ("prefix".equals(type)) {
      for (Attribute attribute : foreignKey) {
        referenceAttrList.add(attribute.getAttrName());
      }
    } else {
      for (Attribute attribute : foreignKey.getReferencedAttrGroup()) {
        referenceAttrList.add(attribute.getAttrName());
      }
    }

    return String.format(
        "foreign key(%s) references %s(%s)",
        String.join(", ", foreignAttrList),
        referenceTableName,
        String.join(", ", referenceAttrList));
  }

  @Override
  public String writePredicate(int isolation, Operation operation, List<AttrValue> attrValues) {
    return null;
  }
}
