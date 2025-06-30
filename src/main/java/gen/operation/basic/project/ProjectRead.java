package gen.operation.basic.project;

import config.schema.OperationConfig;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.view.View;
import java.util.ArrayList;
import java.util.List;
import symbol.Symbol;

public class ProjectRead extends Project {

  /** 当前project read所基于的attribute list */
  public ProjectRead(
      AbstractTable fromClause,
      OperationConfig.ProjectConfig projectConfig,
      ProjectMode primaryKeyOnly) {
    // 直接获得投影属性即可
    super(fromClause, projectConfig, primaryKeyOnly);

    if (fromClause instanceof View) {
      View view = (View) fromClause;

      for (ForeignKey foreignKey : fromClause.getForeignKeyList()) {
        if (view.findAllInfo(foreignKey.get(0)).size() == 2) {
          this.getAttributeGroupInfo().add(Symbol.FK_ATTR_PREFIX + '_' + foreignKey.getId());
          this.getBaseAttributeList().addAll(foreignKey);
        }
      }
    }
  }

  public ProjectRead(
      List<Attribute> baseAttributeList,
      List<String> rightValueList,
      List<String> attributeGroupInfo) {
    super(baseAttributeList, rightValueList, attributeGroupInfo);
  }

  /** @return 返回投影的属性组名，用逗号连接 */
  public String toString() {
    return toSQL();
  }

  @Override
  public String toSQL() {
    List<String> attributeList = new ArrayList<>();
    for (Attribute attribute : this.getBaseAttributeList()) {
      attributeList.add(String.format("`%s`", attribute.getAttrName()));
    }
    return String.join(", ", attributeList);
  }
}
