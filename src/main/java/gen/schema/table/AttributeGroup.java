package gen.schema.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import symbol.Symbol;

@Getter
@Setter
@ToString
public class AttributeGroup extends ArrayList<Attribute> implements Serializable {
  private boolean index;
  private int id;

  public AttributeGroup() {
    super();
  }

  public AttributeGroup(int id, List<Attribute> attrList) {
    super(attrList);
    this.id = id;
  }

  public static String composeCommAttrName(int groupId, int attrId) {
    return String.format("%s%d_%s", Symbol.COMM_ATTR_PREFIX, groupId, attrId);
  }
}
