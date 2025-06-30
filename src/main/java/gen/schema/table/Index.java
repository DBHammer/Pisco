package gen.schema.table;

import config.schema.IndexConfig;
import gen.data.type.DataType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.Getter;
import util.xml.Seed;

@Data
public class Index implements Serializable {
  private static final long serialVersionUID = 1L;

  @Getter private PrimaryKey primaryKey;
  private ArrayList<ForeignKey> foreignKeys;
  private ForeignKey primaryKey2ForeignKey;
  private int indexNum;
  // 索引所包含的属性列
  private List<Attribute> attributeList;

  public Index(
      IndexConfig indexConfig,
      PrimaryKey primaryKey,
      ForeignKey pk2ForeignKey,
      List<ForeignKey> commonForeignKeyList,
      List<AttributeGroup> attributeGroupList) {
    super();

    this.attributeList = new ArrayList<>();
    this.indexNum = 0;
    // 先处理主索引,一定建立
    if (!primaryKey.isEmpty()) { // 要有主键才加索引
      primaryKey.setPrimaryKeyIndex(true);
      this.primaryKey = primaryKey;
      this.attributeList.addAll(primaryKey);
      indexNum++;
    }

    // 设置主键前缀外键索引
    if (pk2ForeignKey != null) {
      pk2ForeignKey.setIndex(true);
      primaryKey2ForeignKey = pk2ForeignKey;
      this.attributeList.addAll(primaryKey2ForeignKey);
      indexNum++;
    }
    // 设置普通外键索引
    this.foreignKeys = new ArrayList<>();
    for (ForeignKey foreignKey : commonForeignKeyList) {
      foreignKey.setIndex(true);
      this.foreignKeys.add(foreignKey);
      this.attributeList.addAll(foreignKey);
      indexNum++;
    }

    // 最后处理非键值属性上的二级索引，加强为以组为单位建索引
    Seed secondarySeed = indexConfig.getIndexSeed();
    for (AttributeGroup attributeGroup : attributeGroupList) {
      // 不是所有数据类型都可以建索引的,在blob上建立索引会出错，一般也没有在blob上建立索引的习惯
      boolean haveBlob = false;
      for (Attribute attribute : attributeGroup) {
        if (attribute.getAttrType() == DataType.BLOB) {
          haveBlob = true;
          break;
        }
      }
      // 如果全组内没有blob，考虑建索引
      if (!haveBlob && secondarySeed.getRandomValue() == 1) {
        attributeGroup.setIndex(true);
        this.attributeList.addAll(attributeGroup);
        indexNum++;
      }
    }
  }

  public Index(PrimaryKey primaryKey, List<ForeignKey> commonForeignKeyList) {
    super();

    this.attributeList = new ArrayList<>();
    this.indexNum = 0;
    // 先处理主索引,一定建立
    if (!primaryKey.isEmpty()) { // 要有主键才加索引
      primaryKey.setPrimaryKeyIndex(true);
      this.primaryKey = primaryKey;
      this.attributeList.addAll(primaryKey);
      indexNum++;
    }

    // 设置普通外键索引
    this.foreignKeys = new ArrayList<>();
    for (ForeignKey foreignKey : commonForeignKeyList) {
      foreignKey.setIndex(true);
      this.foreignKeys.add(foreignKey);
      this.attributeList.addAll(foreignKey);
      indexNum++;
    }
  }

  public boolean isInIndex(Attribute attribute) {
    return this.attributeList.contains(attribute);
  }
}
