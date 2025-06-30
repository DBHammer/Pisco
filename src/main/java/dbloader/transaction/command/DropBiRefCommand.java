package dbloader.transaction.command;

import gen.shadow.Record;
import gen.shadow.Reference;

/** 删除双向引用命令 */
public class DropBiRefCommand implements Command {

  private final Record refFromRecord;
  private final Record refToRecord;
  private final String fromKey;
  private final String toKey;
  private final Reference oldFromRef;
  private final Reference oldToRef;

  public DropBiRefCommand(Record refFromRecord, Record refToRecord, String fromKey, String toKey) {
    this.refFromRecord = refFromRecord;
    this.refToRecord = refToRecord;
    this.fromKey = fromKey;
    this.toKey = toKey;

    oldFromRef = refFromRecord.getRefInfo().refFromMap.get(fromKey);
    oldToRef = refToRecord.getRefInfo().refToMap.get(toKey);
  }

  @Override
  public void execute() {
    refFromRecord.getRefInfo().refFromMap.remove(fromKey);
    refToRecord.getRefInfo().refToMap.remove(toKey);
  }

  @Override
  public void undo() {
    refFromRecord.getRefInfo().refFromMap.put(fromKey, oldFromRef);
    refToRecord.getRefInfo().refToMap.put(toKey, oldToRef);
  }
}
