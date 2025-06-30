package gen.schema.view;

import gen.operation.param.ParamInfo;
import gen.schema.generic.AbstractTable;
import gen.schema.table.*;
import java.io.Serializable;
import java.util.*;
import lombok.Getter;
import org.jgrapht.alg.util.Pair;
import org.jgrapht.graph.DirectedPseudograph;
import symbol.Symbol;
import util.xml.Seed;
import util.xml.SeedUtils;

public class View extends AbstractTable implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final int MAX_SEARCH_JOIN_PATH = 10;
  private static final int MAX_RANDOM_WALK_RANGE = -1;

  // 存储每个 Attr 对应的 ParamInfo
  private final Map<Attribute, ParamInfo> paramInfoMap;

  // 视图的join信息
  @Getter private final Join join;
  private final List<Pair<Table, ForeignKey>> joinPath;

  public View(
      int viewID, String viewName, DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
    super(viewID, viewName);

    paramInfoMap = Collections.synchronizedMap(new HashMap<>());

    // 0.生成join的路径，按参考-被参考的顺序。采用ArrayList<Pair<Table, ForeignKey>>储存，
    // 每个pair存路径上一条边的起点和边。因为外键里有被参考表的信息，不需要存终点。
    this.joinPath = this.generateJoinPath(foreignKeyDependency);
    // 0.1可能无法找到路径，此时直接结束,注意，如果size等于1，相当于只有终点，也是不行的
    if (joinPath == null || joinPath.size() == 1) {
      this.join = null;
      return;
    }
    // 因为dfs从栈里出来是逆序的，要重新反一下才是正常顺序
    Collections.reverse(joinPath);

    // 1.先形成join//这里目前只考虑自然连接，其他类型的连接也可以实现，需要修改代码 TODO
    this.join = new Join(joinPath);
    List<Table> joinTables = this.join.getJoinTables();

    // 下面所有attribute的命名规律是 view + viewID + _ + table_number + _ + table name + _ + attribute name
    // 这里保留的attribute的思路是这样的，除了被参考的主键以外的所有属性均保留。
    // 具体是这么做的：对于原来的主键，只保留起点的表的主键，因为后面的表的主键必然被参考；然后外键（包括前缀外键）和非键属性加上去就完事了

    // 2.再确定join之后形成的虚拟表的primaryKey，即第一个表的primaryKey
    // 这里的每一个attribute我都要重新new一个，因为table中attribute不能直接用在view中，attribute的名字不同
    AttributeGroup pkAttrGroup = new AttributeGroup();
    int pkAttrId = 0;
    for (Attribute attr : joinTables.get(0).getPrimaryKey()) {
      String attrName = Symbol.PK_ATTR_PREFIX + pkAttrId++;
      Attribute newAttr = new Attribute(attr.getAttrId(), attrName, attr.getAttrType());
      pkAttrGroup.add(newAttr);

      Table table = joinTables.get(0);
      paramInfoMap.put(newAttr, new ParamInfo(table, table.getPrimaryKey(), attr));
    }
    this.setPrimaryKey(new PrimaryKey(pkAttrGroup));

    // 3.再确定join之后形成的虚拟表的foreignKey
    List<ForeignKey> commonForeignKeys = new ArrayList<>();
    // 3.3添加每个表的普通外键
    for (Table table : joinTables) {
      for (ForeignKey foreignKey : table.getCommonForeignKeyList()) {
        // 这里的每一个attribute我都要重新new一个，因为table中attribute不能直接用在view中，
        // attribute的名字不同
        ForeignKey newFK =
            new ForeignKey(idHelper.nextFkId(), foreignKey.getReferencedTable(), foreignKey);
        commonForeignKeys.add(newFK);

        for (Attribute attribute : newFK) {
          paramInfoMap.put(
              attribute,
              new ParamInfo(table, foreignKey, newFK.findReferencedAttrByFkAttr(attribute)));
        }
      }
    }

    // 3.4形成最终的view的外键list
    this.setCommonForeignKeyList(commonForeignKeys);

    // 4.再确定join之后形成的虚拟表的attributes，即每个表的attributes添加进去
    // 这里的每一个attribute我都要重新new一个，因为table中attribute不能直接用在view中，attribute的名字不同
    List<AttributeGroup> attributeGroupList = new ArrayList<>();
    for (Table table : joinTables) { // 针对每个表
      AttributeGroup attributeGroup = new AttributeGroup();
      int attributeID = 0;
      int groupId = idHelper.nextAttrGroupId();
      for (Attribute attribute : table.getAttributeList()) {
        Attribute newAttr =
            new Attribute(
                attributeID,
                Symbol.COMM_ATTR_PREFIX + groupId + "_" + attributeID++,
                attribute.getAttrType());
        attributeGroup.add(newAttr);

        // paramFillInfo
        // 前缀外键
        paramInfoMap.put(newAttr, new ParamInfo(table, attribute));
      }
      attributeGroupList.add(attributeGroup);
    }
    this.setAttributeGroupList(attributeGroupList);
  }

  /**
   * 通过随机游走的方式获得外键依赖关系的路径
   *
   * @param foreignKeyDependency 外键参考关系的图，表是顶点，外键是边
   * @return 外键依赖关系的路径
   */
  private List<Pair<Table, ForeignKey>> generateJoinPath(
      DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
    // 1.获得表数量（路径长度）、起始位置、出边选择三个种子
    List<Table> allTable = new ArrayList<>(foreignKeyDependency.vertexSet());
    // 表的数量不够的话直接结束
    if (allTable.size() < 2) {
      return null;
    }
    Seed lengthSeed = SeedUtils.initSeed(1, allTable.size());
    Seed startTableSeed = SeedUtils.initSeed(0, allTable.size() - 1);
    // 终点由每次选择的时候边的数量确定
    Seed selectOuterEdgeSeed = SeedUtils.initSeed(0, 1);
    // 2.搜索的深度就是路径的长度
    int depth = lengthSeed.getRandomValue();
    // 路径用(起点，路径)表示，因为外键储存了被参考的表，所以不用记录终点
    List<Pair<Table, ForeignKey>> ans;
    int i = 0;

    // 3.进行若干次随机游走，如果一直找不到就结束
    while (i++ < MAX_SEARCH_JOIN_PATH) {
      // 3.1获取起始表
      int tableIndex = startTableSeed.getRandomValue();
      // 防止溢出
      while (tableIndex >= allTable.size()) {
        tableIndex = startTableSeed.getRandomValue();
      }
      Table startTable = allTable.get(tableIndex);
      // 3.2进行随机游走，算法本质是dfs的变体
      ans = dfs(depth, startTable, selectOuterEdgeSeed, foreignKeyDependency);
      // 4.如果能得到结果就返回
      if (ans != null && ans.size() > 1) {
        return ans;
      }
    }
    return null;
  }

  /**
   * 查找view属性对应表中的所有属性
   *
   * @param attribute attribute
   * @return view属性对应表中的所有属性
   */
  public List<ParamInfo> findAllInfo(Attribute attribute) {
    List<ParamInfo> ans = new ArrayList<>();

    ParamInfo sourcePara = paramInfoMap.get(attribute);
    Attribute sourceAttribute = sourcePara.attr;
    ans.add(sourcePara);
    // 扫描连接路径，只有在这条路径上的外键可能提供被覆盖的属性，也即外键所参考的属性
    for (Pair<Table, ForeignKey> join : this.joinPath) {
      // 如果不是对应的表，跳过
      if (join.getSecond() == null
          || join.getFirst().getTableId() != sourcePara.table.getTableId()) {
        continue;
      }
      ForeignKey edge = join.getSecond();
      // 如果找不到就直接返回
      if (edge.findReferencedAttrByFkAttr(sourceAttribute) == null) {
        return ans;
      }
      // 添加对应的信息，因为至多只可能有两个属性重合，所以找到就直接返回
      ans.add(
          new ParamInfo(
              edge.getReferencedTable(),
              (PrimaryKey) edge.getReferencedAttrGroup(),
              edge.findReferencedAttrByFkAttr(sourceAttribute)));
      return ans;
    }
    return ans;
  }

  public List<ParamInfo> findAllInfo(String attributeName) {
    for (Attribute attribute : this.paramInfoMap.keySet()) {
      if (attribute.getAttrName().equals(attributeName)) {
        return findAllInfo(attribute);
      }
    }
    return null;
  }

  /**
   * 随机游走算法，就是dfs的时候不遍历所有的出边，而是随机选取其中的一部分边
   *
   * @param depth 现在的深度，当深度为零时返回
   * @param start 当前所在的点（表）
   * @param selectOuterEdgeSeed 选择出边的随机数种子
   * @param foreignKeyDependency 外键依赖图
   * @return something
   */
  private List<Pair<Table, ForeignKey>> dfs(
      int depth,
      Table start,
      Seed selectOuterEdgeSeed,
      DirectedPseudograph<Table, ForeignKey> foreignKeyDependency) {
    List<ForeignKey> outerEdge = new ArrayList<>(foreignKeyDependency.outgoingEdgesOf(start));
    List<Pair<Table, ForeignKey>> ans;
    // 0.到达终点时终止，以路径为null做标志
    if (depth == 0 || outerEdge.isEmpty()) {
      ans = new ArrayList<>();
      ans.add(new Pair<>(start, null));
      return ans;
    }
    // 1.1随机选择起点，搜索当前点的出边
    selectOuterEdgeSeed.setEnd(outerEdge.size());
    int startIndex = selectOuterEdgeSeed.getRandomValue();

    // 1.2为了提高随机游走命中的概率，对每个点随机游走其中的一部分出边，而不只是一条。当没有设定参数的时候默认扫描全部的边
    int walk_length = outerEdge.size();

    // 2.搜索对应的边
    for (int i = 0; i < walk_length; ++i) {
      // 2.1注意搜索的起点是受startIndex影响，而非总是从第一条边开始，使用取余实现循环
      ForeignKey edge = outerEdge.get((i + startIndex) % outerEdge.size());
      // 2.2如果这条边被访问过，放弃。因此不会出现重复的依赖
      if (!edge.isVisited()) {
        // 2.3防止重复访问相同的外键，取到结果就返回
        edge.setVisited(true);
        // 2.4递归
        ans = dfs(depth - 1, edge.getReferencedTable(), selectOuterEdgeSeed, foreignKeyDependency);
        edge.setVisited(false);
        // 返回
        if (ans != null) {
          ans.add(new Pair<>(start, edge));
          return ans;
        }
      }
    }
    return null;
  }

  public ParamInfo findParamInfo(Attribute attribute) {
    return paramInfoMap.get(attribute);
  }
}
