package gen.schema.generic;

import gen.schema.table.*;
import io.SQLizable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import lombok.Getter;
import lombok.Setter;

/** 一个抽象的类，table和view都实现这个类 */
public abstract class AbstractTable implements Serializable, SQLizable {
  private static final long serialVersionUID = 1L;

  protected final IdHelper idHelper;

  // abstractTable的标识
  @Getter private final int tableId;
  @Getter private final String tableName;

  // 表的长度
  @Setter @Getter private int tableSize;

  // 主键信息,一旦创建就不允许做任何修改
  @Setter @Getter private PrimaryKey primaryKey;

  // unique keys
  @Setter @Getter private UniqueKey uniqueKey;

  // 没确定外键信息之前，包含所有非主键属性；确定外键之后，只包含非主键和非外键的信息,一旦创建就不允许做任何修改
  @Getter private List<AttributeGroup> attributeGroupList;

  /** -- GETTER -- 获取主键前缀外键 */
  // 外键信息,一旦创建就不允许做任何修改
  // 主键前缀为外键的情况
  @Setter @Getter protected ForeignKey pk2ForeignKey;
  /** -- GETTER -- 获取普通外键 */
  // 普通外键的情况
  @Getter protected List<ForeignKey> commonForeignKeyList;

  public AbstractTable(int tableId, String tableName) {
    super();
    this.tableId = tableId;
    this.tableName = tableName;
    this.idHelper = new IdHelper();
  }

  /**
   * 获取所有类型的外键
   *
   * @return 外键的列表
   */
  public List<ForeignKey> getForeignKeyList() {
    // 有两种类型的外键，这里将其封装到一个结构中
    List<ForeignKey> foreignKeyList = new ArrayList<>();

    if (pk2ForeignKey != null) foreignKeyList.add(pk2ForeignKey);
    foreignKeyList.addAll(this.getCommonForeignKeyList());

    return foreignKeyList;
  }

  /** 获取非键值组成的所有属性列 */
  public List<Attribute> getAttributeList() {
    List<Attribute> attributeList = new ArrayList<>();
    for (AttributeGroup attributeGroup : this.attributeGroupList) {
      attributeList.addAll(attributeGroup);
    }
    return attributeList;
  }

  public Attribute getAttributeByName(String attributeName) {
    // 1.在非键值属性中查找
    for (Attribute attribute : this.getAttributeList()) {
      if (attribute.getAttrName().equals(attributeName)) {
        return attribute;
      }
    }

    // 2.在普通外键中查找
    List<ForeignKey> foreignKeyList = this.getCommonForeignKeyList();
    for (ForeignKey foreignKey : foreignKeyList) {
      for (Attribute attribute : foreignKey) {
        if (attribute.getAttrName().equals(attributeName)) {
          return attribute;
        }
      }
    }

    // 3.在主键中查找
    for (Attribute attribute : this.getPrimaryKey()) {
      if (attribute.getAttrName().equals(attributeName)) {
        return attribute;
      }
    }
    throw new RuntimeException("no this attribute!");
  }

  public void setAttributeGroupList(List<AttributeGroup> attributeGroupList) {
    this.attributeGroupList = Collections.synchronizedList(attributeGroupList);
  }

  public void setCommonForeignKeyList(List<ForeignKey> commonForeignKeyList) {
    this.commonForeignKeyList = Collections.synchronizedList(commonForeignKeyList);
  }

  /**
   * 找到某个 Attribute 所属的普通外键对象（不含前缀外键）
   *
   * @param attr Attribute
   * @return 如果找到返回ForeignKey，否则null
   */
  public ForeignKey findCommFKByAttr(Attribute attr) {
    for (ForeignKey foreignKey : commonForeignKeyList) {
      if (foreignKey.contains(attr)) {
        return foreignKey;
      }
    }
    return null;
  }

  /**
   * 找到某个 Attribute 所属的普通外键对象（不含前缀外键）
   *
   * @param attrName attrName
   * @return 如果找到返回ForeignKey，否则null
   */
  public ForeignKey findCommFKByAttrName(String attrName) {
    for (ForeignKey foreignKey : commonForeignKeyList) {
      for (Attribute attribute : foreignKey) {
        if (attribute.getAttrName().equals(attrName)) {
          return foreignKey;
        }
      }
    }
    return null;
  }

  /**
   * 判断某个主键属性是否属于前缀外键
   *
   * @param attr attr
   * @return 如果是，就返回true
   */
  public boolean isPkAttrOfPk2Fk(Attribute attr) {
    return pk2ForeignKey != null && attr.getAttrId() < pk2ForeignKey.size();
  }

  @Override
  public String toSQL() {
    return String.format("`%s`", tableName);
  }

  public AttributeGroup addNewIndex() {
    List<AttributeGroup> notIndexList = new ArrayList<>();
    for (AttributeGroup attributeGroup : attributeGroupList) {
      if (!attributeGroup.isIndex()) {
        notIndexList.add(attributeGroup);
      }
    }

    if (!notIndexList.isEmpty()) {
      AttributeGroup newIndex = notIndexList.get(new Random().nextInt(notIndexList.size()));
      newIndex.setIndex(true);
      return newIndex;
    } else {
      return null;
    }
  }

  public AttributeGroup getRandomIndex() {
    List<AttributeGroup> indexList = new ArrayList<>();
    for (AttributeGroup attributeGroup : attributeGroupList) {
      if (attributeGroup.isIndex()) {
        indexList.add(attributeGroup);
      }
    }

    if (!indexList.isEmpty()) {
      return indexList.get(new Random().nextInt(indexList.size()));
    } else {
      return null;
    }
  }

  public AttributeGroup removeRandomIndex() {
    AttributeGroup index = getRandomIndex();
    if (index != null) {
      index.setIndex(false);
      return index;
    } else {
      return null;
    }
  }

  public Attribute addNewAttribute() {
    return null;
  }
}
