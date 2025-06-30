package dbloader.transaction;

import static context.OrcaContext.configColl;

import adapter.Adapter;
import dbloader.transaction.command.Command;
import gen.data.format.DataFormat;
import gen.data.type.DataType;
import gen.operation.basic.OperationDDL;
import gen.schema.table.Attribute;
import gen.schema.table.AttributeGroup;
import gen.schema.table.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import lombok.Cleanup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import symbol.Symbol;
import trace.OperationTrace;
import util.jdbc.DataSourceUtils;
import util.rand.RandUtil;

public class DDLLoader extends OperationLoader {

  public static final Logger logger = LogManager.getLogger(DDLLoader.class);

  /**
   * 执行update操作
   *
   * @param operation 本次要执行的操作
   * @param conn 数据库连接
   * @param operationTrace 要将trace记录到这个对象中
   * @return 命令列表
   * @throws SQLException SQLException时抛出
   */
  public static synchronized List<Command> loadDDLOperation(
      OperationDDL operation, Connection conn, OperationTrace operationTrace) throws SQLException {
    @Cleanup Statement stat = conn.createStatement();

    List<Command> commandList = new LinkedList<>();
    RandUtil randUtil = new RandUtil();
    Adapter adapter = DataSourceUtils.getAdapter();
    // 初始化表信息
    Table table = (Table) operation.getTable();
    List<Attribute> columns = table.getAttributeList();

    String tableName = table.getTableName();
    Attribute targetCol;

    String sqlInstance = "";

    switch (operation.getDdlType()) {
      case AddColumn:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            Attribute newCol = table.addNewAttribute();
            String colFormat =
                String.format(
                    "%s %s", newCol.getAttrName(), adapter.typeConvert(newCol.getAttrType()));
            sqlInstance =
                String.format(
                    "alter table %s add column %s after %s",
                    tableName, colFormat, targetCol.getAttrName());
          }
        }
        break;
      case AddIndex:
        { // alter table %s add index col_name {using type}
          AttributeGroup attributeGroup = table.addNewIndex();
          if (attributeGroup != null) {
            String idxName = tableName + Symbol.INDEX_COMM_ATTR + attributeGroup.getId();
            sqlInstance = adapter.createIndex(idxName, tableName, attributeGroup);
          }
        }
        break;
      case ChangeIndex:
        {
          AttributeGroup attributeGroup = table.getRandomIndex();
          if (attributeGroup != null) {
            String idxName = tableName + Symbol.INDEX_COMM_ATTR + attributeGroup.getId();
            sqlInstance =
                String.format("alter table %s alter index %s invisible", tableName, idxName);
          }
        }
        break;
      case RenameIndex:
        {
          AttributeGroup attributeGroup = table.getRandomIndex();
          if (attributeGroup != null) {
            String idxName = tableName + Symbol.INDEX_COMM_ATTR + attributeGroup.getId();
            sqlInstance =
                String.format("alter table %s rename index %s to %s", tableName, idxName, idxName);
          }
        }
        break;
      case DeleteIndex:
        {
          AttributeGroup attributeGroup = table.removeRandomIndex();
          if (attributeGroup != null) {
            String idxName = tableName + Symbol.INDEX_COMM_ATTR + attributeGroup.getId();
            sqlInstance = String.format("alter table %s drop index %s", tableName, idxName);
          }
        }
        break;
      case ChangeColumn:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            String colName = targetCol.getAttrName();
            DataType dataType = extendDataType(targetCol.getAttrType());

            sqlInstance =
                String.format(
                    "alter table %s change %s %s %s",
                    tableName, colName, colName, adapter.typeConvert(dataType));
          }
        }
        break;
      case RenameColumn:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            String oldName = targetCol.getAttrName();
            targetCol.setAttrName(oldName + "X");

            sqlInstance =
                String.format(
                    "alter table %s rename column %s to %s",
                    tableName, oldName, targetCol.getAttrName());
            // TODO 未处理对SQL语句的影响
          }
        }
        break;
      case DeleteColumn:
        {
          Attribute attribute = table.removeRandomColumn();
          if (attribute != null) {
            String colName = attribute.getAttrName();
            sqlInstance = String.format("alter table %s drop column %s", tableName, colName);
          }
        }
        break;
      case AddPrimaryKey:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            String colName = targetCol.getAttrName();
            sqlInstance = String.format("alter table %s add primary key (%s)", tableName, colName);
          }
        }
        break;

      case AddNullConstraint:
        {
          // 随机选择一个列添加 NULL 约束
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            sqlInstance =
                String.format(
                    "alter table %s modify %s %s null",
                    tableName,
                    targetCol.getAttrName(),
                    adapter.typeConvert(targetCol.getAttrType()));
          }
        }
        break;

      case AddNotNullConstraint:
        {
          // 随机选择一个列添加 NOT NULL 约束
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            sqlInstance =
                String.format(
                    "alter table %s modify %s %s not null",
                    tableName,
                    targetCol.getAttrName(),
                    adapter.typeConvert(targetCol.getAttrType()));
          }
        }
        break;

      case AddUniqueConstraint:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            String colName = targetCol.getAttrName();
            sqlInstance = String.format("alter table %s add unique (%s)", tableName, colName);
          }
        }
        break;

      case ChangeDefaultValue:
        {
          if (!columns.isEmpty()) {
            targetCol = columns.get(randUtil.nextInt(columns.size()));
            String colName = targetCol.getAttrName();
            // 假设有方法获取或设置默认值，例如targetCol.getDefaultValue()
            DataType dataType = targetCol.getAttrType();
            String defaultValue = castDefaultValue(dataType, randUtil.nextInt());
            sqlInstance =
                String.format(
                    "alter table %s alter column %s set default %s",
                    tableName, colName, defaultValue);
          }
        }
        break;
      case ChangeAutoIncrement:
        {
          sqlInstance =
              String.format("alter table %s AUTO_INCREMENT = %d", tableName, randUtil.nextInt());
          break;
        }
      case ForceRebuild:
        {
          sqlInstance = String.format("alter table %s force", tableName);
          break;
        }
      case SetCharSet:
      case ConvertCharSet:
      case ChangeTable:
      default:
        sqlInstance = "";
        break;
    }

    operationTrace.setSql(sqlInstance);
    logger.info(
        "Operation ID :{} ; SQL : {}", operationTrace.getOperationID(), operationTrace.getSql());

    operationTrace.setStartTime(System.nanoTime());
    try {
      stat.execute(sqlInstance);
    } catch (Exception e) {
      logger.warn(e.getMessage());
    }
    operationTrace.setFinishTime(System.nanoTime());

    return commandList;
  }

  private static String castDefaultValue(DataType dataType, int defaultValue) {
    switch (dataType) {
      case BOOL:
        // 对于布尔类型，返回0或1（表示false或true）
        return String.valueOf(defaultValue % 2);

      case DOUBLE:
        // 对于双精度浮点类型，调整格式化参数并返回默认值
        return String.valueOf(defaultValue + ".0");

      case DECIMAL:
        // 对于十进制类型，调整格式化参数并返回默认值
        return String.valueOf(defaultValue + ".0");

      case INTEGER:
        // 对于整数类型，直接返回默认值的字符串形式
        return String.valueOf(defaultValue);

      case VARCHAR:
        // 对于字符串类型，返回默认值的字符串形式
        return String.valueOf(defaultValue);

      case TIMESTAMP:
        // 对于时间戳类型，返回默认值的字符串形式（可能需要进一步格式化）
        return String.valueOf(defaultValue);

      case BLOB:
        // 对于二进制大对象类型，返回默认值的字符串形式（可能需要进一步处理）
        return String.valueOf(defaultValue);

      default:
        // 对于其他未定义类型，返回默认值的字符串形式
        return String.valueOf(defaultValue);
    }
  }

  private static DataType extendDataType(DataType attrType) {
    DataFormat dataFormat = configColl.getDataFormat();

    switch (attrType) {
      case BOOL:
        return DataType.INTEGER;
      case DOUBLE:
        dataFormat.doubleScale += 1;
        dataFormat.doublePrecision += 2;
        return DataType.DOUBLE;
      case DECIMAL:
        dataFormat.decimalScale += 1;
        dataFormat.decimalPrecision += 2;
        return DataType.DECIMAL;
      case INTEGER:
        return DataType.DECIMAL;
      case VARCHAR:
      case TIMESTAMP:
      case BLOB:
      default:
        return attrType;
    }
  }
}
