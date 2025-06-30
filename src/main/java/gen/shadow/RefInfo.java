package gen.shadow;

import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import java.util.HashMap;
import java.util.Map;

public class RefInfo implements Cloneable {
  /** 发出的引用 tableName.attrName -> Reference */
  public final Map<String, Reference> refFromMap;

  /** 指向这里的的引用 tableName.attrName -> Reference */
  public final Map<String, Reference> refToMap;

  public RefInfo() {
    super();
    refFromMap = new HashMap<>();
    refToMap = new HashMap<>();
  }

  /**
   * 生成key
   *
   * @param fromTable 表
   * @param fromAttr 属性
   * @return key
   */
  public static String fromKey(Table fromTable, Attribute fromAttr) {
    ForeignKey foreignKey = fromTable.findCommFKByAttr(fromAttr);
    return String.format("%s.%s", fromTable.getTableName(), foreignKey.getId());
  }

  public static String toKey(Table fromTable, Attribute fromAttr, int fromPkId) {
    ForeignKey foreignKey = fromTable.findCommFKByAttr(fromAttr);
    return String.format("%s.%s.%d", fromTable.getTableName(), foreignKey.getId(), fromPkId);
  }

  /**
   * 深复制两个map，reference不复制
   *
   * @return copy of reference
   */
  public RefInfo copy() {
    RefInfo refInfo = new RefInfo();
    refInfo.refFromMap.putAll(this.refFromMap);
    refInfo.refToMap.putAll(this.refToMap);

    return refInfo;
  }

  @Override
  public RefInfo clone() {
    RefInfo refInfo = new RefInfo();
    refInfo.refFromMap.putAll(this.refFromMap);
    refInfo.refToMap.putAll(this.refToMap);

    return refInfo;
  }
}
