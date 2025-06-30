package dbloader.transaction.command;

import gen.shadow.PartitionTag;
import gen.shadow.Record;

public class SetTagCommand implements Command {
  private final Record record;
  private final PartitionTag oldTag;
  private final PartitionTag newTag;

  public SetTagCommand(Record record, PartitionTag newTag) {
    super();
    this.oldTag = record.getTag();
    this.record = record;
    this.newTag = newTag;
  }

  @Override
  public void execute() {
    record.setTag(newTag);
  }

  @Override
  public void undo() {
    record.setTag(oldTag);
  }
}
