package gen.schema.table;

import gen.data.type.DataType;
import java.io.Serializable;
import lombok.Data;

@Data
public class Attribute implements Serializable {
  private static final long serialVersionUID = 1L;

  private final int attrId;
  private String attrName;
  private final DataType attrType;

  /** 当前attribute索引的两种状态 true:有索引 false:没有索引 */
  private boolean secondaryIndex;

  public Attribute(int attributeID, String attributeName, DataType attrType) {
    super();
    this.attrId = attributeID;
    this.attrName = attributeName;
    this.attrType = attrType;
  }
}
