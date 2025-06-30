package gen.schema;

import adapter.Adapter;
import config.schema.SchemaConfig;
import config.schema.TableConfig;
import gen.schema.table.AttributeGroup;
import gen.schema.table.ForeignKey;
import gen.schema.table.PrimaryKey;
import gen.schema.table.Table;
import gen.schema.view.View;
import io.IOUtils;
import io.SQLizable;
import io.Storable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.jgrapht.graph.DirectedPseudograph;
import symbol.Symbol;
import util.jdbc.DataSourceUtils;
import util.xml.Seed;

@EqualsAndHashCode(callSuper = true)
@Data
public class Schema extends Storable implements SQLizable {
  private static final long serialVersionUID = 1L;
  private List<Table> tableList;
  private List<View> viewList;
  private DirectedPseudograph<Table, ForeignKey> foreignKeyDependency;

  private List<String> sqlList = null;

  /**
   * 除了tableState之外，整个schema一旦创建，就不允许修改
   *
   * @param schemaConfig Document of schemaConfig
   */
  public Schema(SchemaConfig schemaConfig) {
    super();

    // 先确定一个schema有多少张表
    Seed numberTableSeed = schemaConfig.getTableNumber();
    int numberTable = numberTableSeed.getRandomValue();
    if (numberTable == 0) throw new RuntimeException("number of table can not be 0!");

    foreignKeyDependency = new DirectedPseudograph<>(ForeignKey.class);

    TableConfig tableConfig = schemaConfig.getTable();
    // 创建每一张表
    // 构造函数会创建主键和普通属性
    // 外键需要在所有表创建结束再创建
    tableList = Collections.synchronizedList(new ArrayList<>(numberTable));
    for (int i = 0; i < numberTable; i++) {
      Table temp = new Table(i, Symbol.TABLE_NAME_PREFIX + i, tableConfig);
      tableList.add(temp);
      foreignKeyDependency.addVertex(temp);
    }

    // 生成外键
    for (Table table : tableList) {
      table.generateForeignKeys(tableConfig, tableList, foreignKeyDependency);
    }

    // 生成索引
    for (Table table : tableList) {
      table.generateIndex(tableConfig);
    }

    // 确定一个schema有多少个视图
    Seed numberViewSeed = schemaConfig.getTableNumber();
    int numberView = numberViewSeed.getRandomValue();

    // 逐个确定每个视图的信息
    viewList = Collections.synchronizedList(new ArrayList<>(numberView));
    for (int i = 0; i < numberView; i++) {
      View temp = new View(i, Symbol.VIEW_NAME_PREFIX + i, foreignKeyDependency);
      // 有可能得不到路径，此时无法生成视图
      if (temp.getJoin() != null) {
        viewList.add(temp);
      }
    }
  }

  public Schema(
      List<Table> tableList,
      List<View> viewList,
      DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
    this.tableList = tableList;
    this.viewList = viewList;
    this.foreignKeyDependency = foreignKeyDependency;
  }

  public Table findTableById(int tableID) {
    for (Table table : tableList) {
      if (table.getTableId() == tableID) return table;
    }
    throw new RuntimeException("no such tableID......");
  }

  /**
   * 生成 schema 对应的 SQL 语句
   *
   * @return SQL语句数组，每个string对应一条
   */
  public List<String> toSQLList() {
    if (sqlList != null) {
      return sqlList;
    }

    Adapter adapter = DataSourceUtils.getAdapter();

    // 每行一个 sql 语句
    List<String> sqlList = new ArrayList<>();

    PrimaryKey primaryKey;
    ForeignKey primaryKey2ForeignKey;
    List<ForeignKey> commonForeignKeys;
    List<AttributeGroup> attributeGroupList;

    for (Table table : this.getTableList()) {
      sqlList.add(adapter.createTable(table));

      // 输出索引相关信息
      // 1创建主键索引
      primaryKey = table.getPrimaryKey();
      if (primaryKey.isPrimaryKeyIndex()) {
        sqlList.add(
            adapter.createIndex(
                table.getTableName() + Symbol.INDEX_PK, table.getTableName(), primaryKey));
      }

      // 2创建主键前缀外键索引
      primaryKey2ForeignKey = table.getPk2ForeignKey();
      if (primaryKey2ForeignKey != null) {
        sqlList.add(
            adapter.createIndex(
                table.getTableName() + Symbol.INDEX_PK2FK,
                table.getTableName(),
                primaryKey2ForeignKey));
      }

      // 3创建普通外键索引
      commonForeignKeys = table.getCommonForeignKeyList();
      int i = 0;
      for (ForeignKey foreignKey : commonForeignKeys) {
        sqlList.add(
            adapter.createIndex(
                table.getTableName() + Symbol.INDEX_FK + i, table.getTableName(), foreignKey));
        ++i;
      }

      // 4，以组为单位，创建非键值属性的外键索引
      i = 0;
      attributeGroupList = table.getAttributeGroupList();
      for (AttributeGroup attributeGroup : attributeGroupList) {
        if (attributeGroup.isIndex()) {
          sqlList.add(
              adapter.createIndex(
                  table.getTableName() + Symbol.INDEX_COMM_ATTR + attributeGroup.getId(),
                  table.getTableName(),
                  attributeGroup));
          ++i;
        }
      }
    }

    for (Table table : this.getTableList()) {
      // 追加两种外键
      sqlList.addAll(adapter.createForeignKey(table));
    }

    // string.append(schema.getForeignKeyDependency().searchPath().toString()).append("\n");
    for (View view : this.getViewList()) {
      sqlList.add(adapter.createView(view));
    }

    this.sqlList = sqlList;
    return this.sqlList;
  }

  public void writeSQL(String dest) throws IOException {
    IOUtils.writeString(this.toSQL(), dest, false);
  }

  @Override
  public String toSQL() {
    return String.join("\n", this.toSQLList());
  }
}
