package gen.schema.generic;

import java.io.Serializable;

public class IdHelper implements Serializable {
  /** 下一个可用attrGroupId */
  private int availableAttrGroupId = 0;

  /** 下一个可用fkId（标识ForeignKey本身） */
  private int availableFkId = 0;

  /**
   * 提供下一个可用的AttrGroupId并指向下一个
   *
   * @return next available AttrGroupId
   */
  public int nextAttrGroupId() {
    return availableAttrGroupId++;
  }

  /**
   * 提供下一个可用的fkId并指向下一个
   *
   * @return next available fkId
   */
  public int nextFkId() {
    return availableFkId++;
  }
}
