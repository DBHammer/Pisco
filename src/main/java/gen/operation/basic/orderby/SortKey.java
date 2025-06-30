package gen.operation.basic.orderby;

import config.schema.OperationConfig;
import gen.operation.basic.project.Project;
import gen.schema.table.Attribute;
import io.SQLizable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import util.xml.Seed;

public class SortKey implements SQLizable, Serializable {
  /** 当前order by所基于的attribute list */
  private final List<Attribute> baseAttributeList;

  public SortKey(Project project, OperationConfig.SortKeyConfig sortKeyConfig) {

    // 排序的attribute一定来自于投影属性，简单地考虑取投影属性的前一部分进行排序
    List<Attribute> candidateAttribute = project.getBaseAttributeList();
    if (sortKeyConfig.getIfSortSeed().isAvailable()
        && sortKeyConfig.getIfSortSeed().getRandomValue() == 0) {
      this.baseAttributeList = new ArrayList<>();
      return;
    }
    // 限制随机数生成范围
    Seed sortSeed = sortKeyConfig.getKeySeed();
    sortSeed.setRange(0, candidateAttribute.size());
    int endIndex = sortSeed.getRandomValue();
    this.baseAttributeList = new ArrayList<>(candidateAttribute.subList(0, endIndex));
  }

  public SortKey(List<Attribute> baseAttributeList) {
    this.baseAttributeList = baseAttributeList;
  }

  /** @return 如果不存在排序属性，返回一个空串；否则返回order by+属性表 */
  public String toString() {
    return toSQL();
  }

  @Override
  public String toSQL() {
    if (this.baseAttributeList.isEmpty()) {
      return "";
    }

    ArrayList<String> attributeList = new ArrayList<>();
    for (Attribute attribute : this.baseAttributeList) {
      attributeList.add(String.format("`%s`", attribute.getAttrName()));
    }
    return String.format("order by %s", String.join(", ", attributeList));
  }
}
