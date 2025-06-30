package gen.schema.table;

import config.schema.UniqueKeyConfig;
import gen.data.type.DataType;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import symbol.Symbol;
import util.xml.Seed;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class UniqueKey extends AttributeGroup implements Serializable {
  private static final long serialVersionUID = 1L;

  private boolean uniqueKeyIndex;

  public UniqueKey(UniqueKeyConfig uniqueKeyConfig) {
    super();

    Seed lengthSeed = uniqueKeyConfig.getAttributeNumber();
    int lengthUniqueKey = lengthSeed.getRandomValue();

    Seed typeSeed = uniqueKeyConfig.getUkTypeSeed();
    for (int i = 0; i < lengthUniqueKey; i++) {
      this.add(
          new Attribute(
              i, composeUkAttrName(i), DataType.dataTypeConst2enum(typeSeed.getRandomValue())));
    }

    if (lengthUniqueKey != this.size()) {
      throw new RuntimeException("unique key length error!");
    }
  }

  public String composeUkAttrName(int attrId) {
    return String.format("%s%s", Symbol.UK_ATTR_PREFIX, attrId);
  }
}
