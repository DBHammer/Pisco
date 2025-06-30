package gen.schema.table;

import config.schema.AttributeConfig;
import config.schema.ForeignKeyConfig;
import config.schema.TableConfig;
import context.OrcaContext;
import gen.data.type.DataType;
import gen.schema.generic.AbstractTable;
import java.io.Serializable;
import java.util.*;
import lombok.Getter;
import org.jgrapht.graph.DirectedPseudograph;
import symbol.Symbol;
import util.xml.Seed;

@Getter
public class Table extends AbstractTable implements Serializable {
  private static final long serialVersionUID = 1L;

  // 索引信息
  private Index index;

  private Partition partition = null;

  @Getter private String rowFormat = "";

  /**
   * 构造函数
   *
   * @param tableID id
   * @param tableName name
   */
  public Table(int tableID, String tableName, TableConfig tableConfig) {
    super(tableID, tableName);

    // 1.先确定主键信息
    this.setPrimaryKey(new PrimaryKey(tableConfig.getPrimaryKey()));

    this.setUniqueKey(new UniqueKey(tableConfig.getUniqueKey()));

    // 2.再确定非主键信息
    this.buildCommonAttrGroups(tableConfig.getAttribute());

    // 3.根据xml，确定表的长度
    Seed lengthSeed = tableConfig.getRecordNumber();
    this.setTableSize(lengthSeed.getRandomValue());

    // 4.分区
    if (tableConfig.isUsePartition()) {
      partition = new Partition(this.getPrimaryKey(), tableName);
    } else {
      partition = null;
    }

    // 5.元数据 (eg. rowFormat)
    if (tableConfig.isUseRowFormat()) {
      String[] rowFormatOptions = {"COMPACT", "DYNAMIC", "REDUNDANT"};
      Random random = new Random();
      int randomIndex = random.nextInt(rowFormatOptions.length);
      rowFormat = "ROW_FORMAT = " + rowFormatOptions[randomIndex];
    }
  }

  /**
   * 获取此表依赖的所有表
   *
   * @return set of tables which depended by this table
   */
  public Set<Table> getDependTableSet() {
    Set<Table> tableSet = new HashSet<>();
    if (pk2ForeignKey != null) {
      tableSet.add(pk2ForeignKey.getReferencedTable());
    }
    for (ForeignKey fk : commonForeignKeyList) {
      tableSet.add(fk.getReferencedTable());
    }
    return tableSet;
  }

  public void generateIndex(TableConfig tableConfig) {
    // 6.确定索引信息
    this.index =
        new Index(
            tableConfig.getIndexConfig(),
            this.getPrimaryKey(),
            this.getPk2ForeignKey(),
            this.getCommonForeignKeyList(),
            this.getAttributeGroupList());
  }

  public void generateForeignKeys(
      TableConfig tableConfig,
      List<Table> hasBuildTables,
      DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
    // 4.再确定外键信息，并最终确定确定非键值属性信息，因为要排除一些成为外键的attribute
    this.initForeignKeys(tableConfig.getForeignKey(), hasBuildTables);

    // 5.根据形成好的外键建立，外键依赖图;
    if (getPk2ForeignKey() != null) {
      foreignKeyDependency.addEdge(
          this, getPk2ForeignKey().getReferencedTable(), getPk2ForeignKey());
    }
    for (ForeignKey foreignKey : this.getCommonForeignKeyList()) {

      foreignKeyDependency.addEdge(this, foreignKey.getReferencedTable(), foreignKey);
    }
  }

  public void initForeignKeys(ForeignKeyConfig foreignKeyConfig, List<Table> hasBuildTables) {

    // 忽略前缀外键
    this.pk2ForeignKey = null;
    this.setCommonForeignKeyList(buildCommonForeignKey(foreignKeyConfig, hasBuildTables));
  }

  /**
   * 根据xml，为当前表建立普通外键
   *
   * @param foreignKeyConfig 对应内容的生成参数config
   * @param hasBuildTables 已经生成的表，也就是全部的表
   * @return ArrayList<ForeignKey>
   */
  private List<ForeignKey> buildCommonForeignKey(
      ForeignKeyConfig foreignKeyConfig, List<Table> hasBuildTables) {

    // 1.1根据xml，形成选择生成多少个外键的结构
    Seed numberCommonForeignKeySeed = foreignKeyConfig.getForeignKeyNumber();
    int numberCommonForeignKey = numberCommonForeignKeySeed.getRandomValue();

    // 2.生成numberCommonForeignKey数量个外键
    // 注意：本节代码以下内容中，foreign/this对应当前表，refer(ence)对应被参考的表，如此命名是因为一下子想不到合适的对应词，建议后续进行修改 TODO

    // 参考表的生成种子
    // 强制要求种子的范围是0-size
    Seed referencedTableSeed = foreignKeyConfig.getReferenceTableSeed();
    referencedTableSeed.setRange(0, hasBuildTables.size());

    ArrayList<ForeignKey> commonForeignKeys = new ArrayList<>();
    // 3 选择外键组和参考的属性组，这里全部的操作都是以组为单位的
    for (int i = 0; i < numberCommonForeignKey; ++i) {
      // 3.1 准备工作
      // 获取参考表的选择起点
      int referencedTableIndex = referencedTableSeed.getRandomValue();
      // 获取被参考的表
      Table referencedTable = hasBuildTables.get(referencedTableIndex);

      // TODO 现在排除了自身依赖和互相依赖
      if (referencedTable.getTableId() >= this.getTableId()) {
        continue;
      }

      // 3.2 使用被参考的主键构造外键
      PrimaryKey referencedPK = referencedTable.getPrimaryKey();
      ForeignKey foreignKey = new ForeignKey(idHelper.nextFkId(), referencedTable, referencedPK);

      // 4把参考的属性组加进外键里
      commonForeignKeys.add(foreignKey);
    }
    return commonForeignKeys;
  }

  public void buildCommonAttrGroups(AttributeConfig attributeConfig) {

    // 先随机选取属性列的数目
    Seed numberSeed = attributeConfig.getAttributeNumber();

    // 确定生成的组数
    Seed groupSeed = attributeConfig.getGroupNumberSeed();
    // attr type
    Seed typeSeed = attributeConfig.getAttrType();

    int groupNumber = groupSeed.getRandomValue();
    List<AttributeGroup> attributeGroupList = new ArrayList<>();

    for (int i = 0; i < groupNumber; ++i) {
      int lenOfGroup = numberSeed.getRandomValue();
      int groupId = idHelper.nextAttrGroupId();

      List<Attribute> attributeList = new ArrayList<>();
      for (int attrId = 0; attrId < lenOfGroup; ++attrId) {
        // 属性名
        String attrName = AttributeGroup.composeCommAttrName(groupId, attrId);
        // 属性类型
        DataType attrType = DataType.dataTypeConst2enum(typeSeed.getRandomValue());

        // 添加新属性
        attributeList.add(new Attribute(attrId, attrName, attrType));
      }
      attributeGroupList.add(new AttributeGroup(groupId, attributeList));
    }

    this.setAttributeGroupList(attributeGroupList);
  }

  @Override
  public Attribute addNewAttribute() {
    Seed typeSeed = OrcaContext.configColl.getSchema().getTable().getAttribute().getAttrType();
    int groupId = idHelper.nextAttrGroupId();
    int attrId = 0;
    List<Attribute> attributeList = new ArrayList<>();
    // 属性名
    String attrName = AttributeGroup.composeCommAttrName(groupId, attrId);
    // 属性类型
    DataType attrType = DataType.dataTypeConst2enum(typeSeed.getRandomValue());

    // 添加新属性
    attributeList.add(new Attribute(attrId, attrName, attrType));

    this.getAttributeGroupList().add(new AttributeGroup(groupId, attributeList));

    return attributeList.get(0);
  }

  public DataType findAttrTypeByAttrName(String attrName) {
    if (attrName.startsWith(Symbol.PK_ATTR_PREFIX)) {
      int pkAttrId = Integer.parseInt(attrName.substring(Symbol.PK_ATTR_PREFIX.length()));
      return this.getPrimaryKey().get(pkAttrId).getAttrType();
    } else if (attrName.startsWith(Symbol.FK_ATTR_PREFIX)) {
      String[] gid_id = attrName.substring(Symbol.FK_ATTR_PREFIX.length()).split("_");
      int fkAttrGroupId = Integer.parseInt(gid_id[0]);
      int fkAttrId = Integer.parseInt(gid_id[1]);
      return this.getCommonForeignKeyList().get(fkAttrGroupId).get(fkAttrId).getAttrType();
    } else if (attrName.startsWith(Symbol.COMM_ATTR_PREFIX)) {
      String[] gid_id = attrName.substring(Symbol.COMM_ATTR_PREFIX.length()).split("_");
      int attrGroupId = Integer.parseInt(gid_id[0]);
      int attrId = Integer.parseInt(gid_id[1]);
      return this.getAttributeGroupList().get(attrGroupId).get(attrId).getAttrType();
    } else {
      throw new RuntimeException("不应该运行到这里吧？");
    }
  }

  public Attribute removeRandomColumn() {
    List<AttributeGroup> attributeGroupList = this.getAttributeGroupList();
    if (attributeGroupList.isEmpty()) {
      return null;
    }
    int index = new Random().nextInt(attributeGroupList.size());
    int colIdx = new Random().nextInt(attributeGroupList.get(index).size());
    Attribute attribute = attributeGroupList.get(index).get(colIdx);
    attributeGroupList.get(index).remove(colIdx);
    if (attributeGroupList.get(index).isEmpty()) {
      attributeGroupList.remove(index);
    }
    return attribute;
  }
}
