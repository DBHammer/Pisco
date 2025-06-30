package gen.operation.basic.project;

import config.schema.OperationConfig;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.AttributeGroup;
import gen.schema.table.ForeignKey;
import gen.schema.table.PrimaryKey;
import io.SQLizable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import symbol.Symbol;
import util.xml.Seed;

@Data
public abstract class Project implements SQLizable, Serializable {
  private List<Attribute> baseAttributeList;
  private List<String> rightValueList;
  private List<String> attributeGroupInfo;

  public Project() {
    super();
  }

  public Project(
      List<Attribute> baseAttributeList,
      List<String> rightValueList,
      List<String> attributeGroupInfo) {
    this.baseAttributeList = baseAttributeList;
    this.rightValueList = rightValueList;
    this.attributeGroupInfo = attributeGroupInfo;
  }

  /**
   * get project target from table in fromClause
   *
   * @param table projection target
   * @param projectConfig project config
   * @param projectMode project mode
   */
  public Project(
      AbstractTable table, OperationConfig.ProjectConfig projectConfig, ProjectMode projectMode) {

    this.attributeGroupInfo = new ArrayList<>();
    this.baseAttributeList = new ArrayList<>();

    // 如果需要主键，则添加
    // 1.如果只投影主键，则不需要后续的流程，可以直接结束
    if (projectMode == ProjectMode.PrimaryKeyOnly) {
      baseAttributeList.addAll(table.getPrimaryKey());
      this.attributeGroupInfo.add(Symbol.PK_ATTR_PREFIX);
      return;
    }

    List<AttributeGroup> attributeGroups = new ArrayList<>();
    //        // 获取非键的所有属性组
    //        List<AttributeGroup> attributeGroups = new ArrayList<>(table.getAttributeGroupList());
    //        // 获取唯一键的所有属性
    //        UniqueKey uniqueKeys = table.getUniqueKey();
    //        boolean hasUK = !uniqueKeys.isEmpty();
    // 随机取一种投影属性组类型0:随机抽取一部分属性 1:抽取全部非键属性 2:抽取全部非主键属性
    //        ProjectGroupNum projectType =
    //                ProjectGroupNum.values()[
    //                        projectConfig.getAttrTypeSeed().getRandomValue()
    //                        ];
    // 加入外键和唯一键
    //        if (projectType == ProjectGroupNum.AllNotPK){
    //            for (ForeignKey foreignKey : table.getForeignKeyList()) {
    //                attributeGroups.add(new AttributeGroup(foreignKey.getId(), foreignKey));
    //            }
    //
    //            //attributeGroups.add(uniqueKeys);
    //        }
    // 随机取一种投影属性组类型0:随机抽取一部分属性 1:抽取全部键值属性 2:抽取全部非键值属性
    ProjectGroupNum projectType =
        ProjectGroupNum.values()[projectConfig.getAttrTypeSeed().getRandomValue()];

    // 如果支持投影全部属性列，就把主键外键加进去，不然的话只加非键值部分
    if (projectMode == ProjectMode.AllowAll && projectType != ProjectGroupNum.AllNotKey) {
      attributeGroups.addAll(
          table.getForeignKeyList().stream()
              .map(f -> new AttributeGroup(f.getId(), f))
              .collect(Collectors.toList()));
      attributeGroups.add(table.getPrimaryKey());
    }
    // 除非指定全部是key，不然把非键值的部分加进去
    if (projectType != ProjectGroupNum.AllKey) {
      attributeGroups.addAll(new ArrayList<>(table.getAttributeGroupList()));
    }

    Seed projectSeed = projectConfig.getAttrGroupSeed();
    projectSeed.setRange(0, attributeGroups.size());

    int groupNum = attributeGroups.size();
    // if (hasUK)  groupNum++;
    // 确定随机抽取的属性组数量，如果是后两种模式，抽取的属性组数量即是全部的数量
    if (projectType == ProjectGroupNum.Random) {
      Seed prejectNumSeed = projectConfig.getAttributeNumber();
      if (prejectNumSeed.getEnd() > attributeGroups.size() + 1)
        prejectNumSeed.setEnd(attributeGroups.size() + 1);
      groupNum = prejectNumSeed.getRandomValue();
    }

    // 3.随机获取一批属性组，并把它加入投影的属性组中
    // 投影的属性组的信息，即组的类型(e.g. primarykey)
    int groupIndex = projectSeed.getRandomValue();
    for (int i = 0; i < groupNum; ++i) {
      //            int index = (groupIndex + i) % attributeGroups.size();
      //            if (hasUK && index == (groupNum-1)){
      //                this.attributeGroupInfo.add(Symbol.UK_ATTR_PREFIX+'_' + uniqueKeys.getId());
      //                baseAttributeList.addAll(uniqueKeys);
      //            }else {
      //                AttributeGroup selectedGroup = attributeGroups.get((groupIndex + i) %
      // attributeGroups.size());
      //                this.attributeGroupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' +
      // selectedGroup.getId());
      //                baseAttributeList.addAll(selectedGroup);
      //            }
      AttributeGroup selectedGroup = attributeGroups.get((groupIndex + i) % attributeGroups.size());
      if (selectedGroup instanceof ForeignKey) {
        this.attributeGroupInfo.add(Symbol.FK_ATTR_PREFIX + '_' + selectedGroup.getId());
      } else if (selectedGroup instanceof PrimaryKey) {
        this.attributeGroupInfo.add(Symbol.PK_ATTR_PREFIX + '_' + selectedGroup.getId());
      } else {
        this.attributeGroupInfo.add(Symbol.COMM_ATTR_PREFIX + '_' + selectedGroup.getId());
      }
      baseAttributeList.addAll(selectedGroup);
    }
  }

  public String toString() {
    return "";
  }
}
