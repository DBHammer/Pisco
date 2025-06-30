package dbloader.transaction.savepoint;

import dbloader.transaction.command.Command;
import gen.shadow.Record;
import java.sql.Savepoint;
import java.util.*;
import lombok.Data;

@Data
public class OrcaSavePoint {
  // 标识符，添加删除SavePoint都需要一个标识符
  private final Savepoint savepoint;
  // 存储截至此SavePoint的PrivateRecordMap
  private final Map<String, Record> privateRecordMap;
  // 存储截至此SavePoint的needLockRecordSet
  private final Set<Record> needLockRecordSet;
  // 存储截至此SavePoint的所有命令
  private final List<Command> commandList;

  public OrcaSavePoint(
      Savepoint savepoint,
      Map<String, Record> privateRecordMap,
      Set<Record> needLockRecordSet,
      List<Command> commandList) {
    super();
    this.savepoint = savepoint;

    // 这些 Record 是私有的，需要 copy 一份
    this.privateRecordMap = new HashMap<>();
    privateRecordMap.forEach((key, record) -> this.privateRecordMap.put(key, record.copy()));

    // 这些 Record 是全局的，应当使用原始数据
    this.needLockRecordSet = new LinkedHashSet<>(needLockRecordSet);

    // commandList中元素不会变动，无需 copy
    this.commandList = new ArrayList<>(commandList);
  }
}
