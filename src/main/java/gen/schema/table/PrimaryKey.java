package gen.schema.table;

import config.schema.PrimaryKeyConfig;
import gen.data.type.DataType;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import symbol.Symbol;
import util.xml.Seed;

@Getter
@Setter
@ToString
public class PrimaryKey extends AttributeGroup implements Serializable {
  private static final long serialVersionUID = 1L;

  private boolean autoIncrease;
  private boolean primaryKeyIndex;

  /**
   * 在view中形成primaryKey
   *
   * @param pkAttrGroup pkAttrGroup
   */
  public PrimaryKey(AttributeGroup pkAttrGroup) {
    super();
    this.addAll(pkAttrGroup);
  }

  /** 在table中生成primaryKey */
  public PrimaryKey(PrimaryKeyConfig primaryKeyConfig) {
    super();
    // 1.根据xml,随机选取主键属性的长度
    Seed lengthSeed = primaryKeyConfig.getAttributeNumber();
    int lengthPrimaryKey = lengthSeed.getRandomValue();

    // 2.再决定每个属性列的类型

    Seed typeSeed = primaryKeyConfig.getPkTypeSeed();
    for (int i = 0; i < lengthPrimaryKey; i++) {
      this.add(
          new Attribute(
              i, composePkAttrName(i), DataType.dataTypeConst2enum(typeSeed.getRandomValue())));
    }

    // 如果只有一个属性列，可以考虑是否自增
    if (lengthPrimaryKey == 1 && this.get(0).getAttrType() == DataType.INTEGER) {
      Seed autoIncreaseSeed = primaryKeyConfig.getIncreaseSeed();
      autoIncrease = autoIncreaseSeed.getRandomValue() == 1;
    }

    if (lengthPrimaryKey != this.size()) {
      throw new RuntimeException("primary key length error!");
    }
  }

  public String composePkAttrName(int attrId) {
    return String.format("%s%s", Symbol.PK_ATTR_PREFIX, attrId);
  }
}
