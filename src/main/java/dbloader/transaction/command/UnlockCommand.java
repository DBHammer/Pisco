package dbloader.transaction.command;

import gen.shadow.Record;
import java.util.Set;

public class UnlockCommand implements Command {
  private final Set<Record> recordSet;

  public UnlockCommand(Set<Record> recordSet) {
    super();
    this.recordSet = recordSet;
  }

  @Override
  public void execute() {
    recordSet.forEach(record -> record.lock.writeLock().unlock());
  }

  @Override
  public void undo() {
    // 并且在undo中进行反向操作
    recordSet.forEach(record -> record.lock.writeLock().lock());
  }
}
