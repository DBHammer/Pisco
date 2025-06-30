package dbloader.transaction.command;

import gen.shadow.Record;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** 这个类目前并不用于正向操作，execute无效，加锁过程穿插在程序各处，该命令只用于撤销 */
public class LockCommand implements Command {

  private final Set<Record> recordSet;

  public LockCommand(Set<Record> recordSet) {
    this.recordSet = recordSet;
  }

  @Override
  public void execute() {
    // 对record按照tableId，pkId进行排序
    // 以确保加锁顺序固定
    List<Record> recordList = new ArrayList<>(recordSet);
    recordList.sort(
        (recordA, recordB) -> {
          if (recordA.getTableId() == recordB.getTableId()) {
            return Integer.compare(recordA.getPkId(), recordB.getPkId());
          } else {
            return Integer.compare(recordA.getTableId(), recordB.getTableId());
          }
        });

    //        for (int i=0; i<100; i++) {
    //            // 如果成功获取锁，则返回
    //            if(tryLockAllRecord(recordList)) {
    //                return;
    //            }
    //        }
    //        // 未成功获取锁，抛出错误
    //        throw new TryLockTimeoutException("无法获取所有写锁以维护MiniShadow，需要回滚");
    recordList.forEach(record -> record.lock.writeLock().lock());
  }

  @Override
  public void undo() {
    // 释放读锁
    recordSet.forEach(record -> record.lock.writeLock().unlock());
  }

  private boolean tryLockAllRecord(List<Record> recordList) {
    Set<Record> lockedRecordSet = new LinkedHashSet<>();
    try {
      // 获取读锁
      for (Record record : recordList) {
        // 逐个获取写锁，一旦出现无法获取的锁，则释放掉所有已获取的锁，并报错
        if (!record.lock.writeLock().tryLock(1, TimeUnit.MICROSECONDS)) {
          lockedRecordSet.forEach(rec -> rec.lock.writeLock().unlock());
          return false;
        }
        lockedRecordSet.add(record);
      }
    } catch (InterruptedException e) {
      lockedRecordSet.forEach(rec -> rec.lock.writeLock().unlock());
      return false;
    }
    return true;
  }
}
