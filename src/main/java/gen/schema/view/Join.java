package gen.schema.view;

import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.jgrapht.alg.util.Pair;

@Data
public class Join implements Serializable {
  private static final long serialVersionUID = 1L;

  // 这个join中表的个数
  private final int lengthJoin;
  // 连接条件
  // <<TableA, columnA>, <TableB, columnB>>
  private final List<Pair<Pair<String, String>, Pair<String, String>>> joinConditions;
  // 需要参与连接的表
  private final List<Table> joinTables;

  public Join(List<Pair<Table, ForeignKey>> joinPath) {
    super();
    // 1.从路径可以直接得到长度
    lengthJoin = joinPath.size();

    // 2.根据选择的path构建join,即形成joinTablesString和joinConditionString这两个结构
    // 所有参与连接的attribute属性，但是不包含任何主键，即joinConditionString等号左边的attribute
    joinTables = new ArrayList<>();
    // 保存参与连接的表的ID
    // 保存参与连接表的主键长度

    // 3.形成joinTablesString
    for (Pair<Table, ForeignKey> tableForeignKeyPair : joinPath) { // 先构建joinTables
      Table temp = tableForeignKeyPair.getFirst();
      // 这两个信息暂时不会被调用到，姑且保留
      joinTables.add(temp);
    }

    // 4.形成joinConditions
    this.joinConditions = new ArrayList<>();
    for (int i = 0; i < joinPath.size() - 1; i++) {
      // 提取起点，边，属性数量的信息
      Table start = joinPath.get(i).getFirst();
      ForeignKey edge = joinPath.get(i).getSecond();
      int attributeLength = edge.size();
      // 因为可能会有重复的表，前缀上需要有一个唯一的序号
      for (int j = 0; j < attributeLength; ++j) {
        Attribute attribute = edge.get(j);
        // 注意，被参考的表的顺序应该正好是当前表的下一个，所以可以直接得到表的序号
        joinConditions.add(
            new Pair<>(
                new Pair<>(start.getTableName(), attribute.getAttrName()),
                new Pair<>(
                    edge.getReferencedTable().getTableName(),
                    edge.getReferencedAttrGroup().get(j).getAttrName())));
      }
    }
  }
}
