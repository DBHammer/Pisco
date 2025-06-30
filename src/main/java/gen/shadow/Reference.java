package gen.shadow;

import gen.schema.table.Attribute;
import gen.schema.table.Table;

/** 存储 reference 信息 */
public class Reference {

  public Reference(
      Table fromTable,
      Attribute fromAttr,
      int fromPkId,
      Table toTable,
      Attribute toAttr,
      int toFkId) {
    super();
    this.fromTable = fromTable;
    this.fromAttr = fromAttr;
    this.fromPkId = fromPkId;
    this.toTable = toTable;
    this.toAttr = toAttr;
    this.toFkId = toFkId;
  }

  /** 引用源自该表 */
  public final Table fromTable;

  /** 引用源自该属性 */
  public final Attribute fromAttr;

  /** 引用发出者 */
  public final int fromPkId;

  /** 引用指向的地方 */
  public final Table toTable;

  /** 被引用属性 */
  public final Attribute toAttr;

  /** 引用指向的地方 */
  public final int toFkId;
}
