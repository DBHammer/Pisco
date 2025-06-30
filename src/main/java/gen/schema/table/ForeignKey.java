package gen.schema.table;

import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import symbol.Symbol;

@Getter
@Setter
@ToString
public class ForeignKey extends AttributeGroup implements Serializable {
  private static final long serialVersionUID = 1L;

  // 外键是参考哪个表的哪个主键?属性
  private final Table referencedTable;
  private final AttributeGroup referencedAttrGroup;

  // 用于生成join时确定这一外键是否被访问。
  private boolean isVisited;

  // 是否建立索引的信息
  private boolean index;

  public ForeignKey(int fkId, Table referencedTable, AttributeGroup referencedAttrGroup) {
    super();
    this.setId(fkId);
    this.referencedTable = referencedTable;
    this.referencedAttrGroup = referencedAttrGroup;

    for (int attrId = 0; attrId < referencedAttrGroup.size(); attrId++) {
      this.add(
          new Attribute(
              attrId,
              composeFkAttrName(fkId, attrId),
              referencedAttrGroup.get(attrId).getAttrType()));
    }
  }

  /**
   * 找到FKAttr所参考的的PKAttr
   *
   * @param attr attribute
   * @return FKAttr所参考的的PKAttr
   */
  public Attribute findReferencedAttrByFkAttr(Attribute attr) {
    if (!this.contains(attr)) return null;
    return referencedAttrGroup.get(this.indexOf(attr));
  }

  public String composeFkAttrName(int fkId, int attrId) {
    return String.format("%s%d_%d", Symbol.FK_ATTR_PREFIX, fkId, attrId);
  }
}
