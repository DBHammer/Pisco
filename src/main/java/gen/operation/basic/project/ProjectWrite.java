package gen.operation.basic.project;

import config.schema.OperationConfig;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import java.util.ArrayList;
import java.util.List;

public class ProjectWrite extends Project {

  public ProjectWrite(
      AbstractTable fromClause,
      OperationConfig.ProjectConfig projectWriteConfig,
      ProjectMode projectMode) {
    super(fromClause, projectWriteConfig, projectMode);

    // 在获得投影属性的基础上，给每个属性对应的右值（新值），暂时用？表示
    ArrayList<String> rightValues = new ArrayList<>();
    for (Attribute ignored : this.getBaseAttributeList()) {
      String rightValue = "?";
      rightValues.add(rightValue);
    }
    this.setRightValueList(rightValues);
  }

  /**
   * 对于指定属性生成投影
   *
   * @param baseAttributeList
   * @param attributeGroupInfo
   */
  public ProjectWrite(List<Attribute> baseAttributeList, List<String> attributeGroupInfo) {
    super();
    this.setAttributeGroupInfo(attributeGroupInfo);
    this.setBaseAttributeList(baseAttributeList);
    List<String> rightValue = new ArrayList<>();
    for (int i = 0; i < baseAttributeList.size(); ++i) {
      rightValue.add("?");
    }
    this.setRightValueList(rightValue);
  }

  @Override
  public String toString() {
    return toSQL();
  }

  @Override
  /*
   @return 返回 "属性名=新值"的列表，用逗号连接
  */
  public String toSQL() {
    ArrayList<String> newValues = new ArrayList<>();
    for (int i = 0; i < this.getRightValueList().size(); ++i) {
      newValues.add(
          String.format(
              "`%s` = %s",
              this.getBaseAttributeList().get(i).getAttrName(), this.getRightValueList().get(i)));
    }
    return String.join(", ", newValues);
  }
}
