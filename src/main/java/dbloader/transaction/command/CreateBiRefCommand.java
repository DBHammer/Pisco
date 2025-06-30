package dbloader.transaction.command;

import gen.shadow.Record;
import gen.shadow.Reference;

/** 建立双向引用命令 */
public class CreateBiRefCommand implements Command {

  private final Record refFromRecord;
  private final Record refToRecord;
  private final String fromKey;
  private final String toKey;
  private final Reference ref;

  public CreateBiRefCommand(
      Record refFromRecord, Record refToRecord, String fromKey, String toKey, Reference ref) {
    super();
    this.refFromRecord = refFromRecord;
    this.refToRecord = refToRecord;
    this.fromKey = fromKey;
    this.toKey = toKey;
    this.ref = ref;
  }

  @Override
  public void execute() {
    refFromRecord.getRefInfo().refFromMap.put(fromKey, ref);
    refToRecord.getRefInfo().refToMap.put(toKey, ref);
  }

  @Override
  public void undo() {
    refFromRecord.getRefInfo().refFromMap.remove(fromKey);
    refToRecord.getRefInfo().refToMap.remove(toKey);
  }
}
