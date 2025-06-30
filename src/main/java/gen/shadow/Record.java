package gen.shadow;

import java.io.Serializable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Data;

@Data
public class Record implements Serializable, Cloneable {

  /** 锁，暂且用一个布尔变量标识，实现时再考虑怎么做 */
  public ReadWriteLock lock;

  private int tableId;

  private int pkId;

  /** 标识分区信息 */
  private PartitionTag tag;

  /** 引用信息 */
  private RefInfo refInfo;

  private Record() {
    super();
    lock = null;
  }

  public Record(int tableId, int pkId, PartitionTag tag, RefInfo refInfo) {
    super();
    this.tableId = tableId;
    this.pkId = pkId;
    this.lock = new ReentrantReadWriteLock();
    this.tag = tag;
    this.refInfo = refInfo;
  }

  /**
   * 复制record，lock置为null，深复制refInfo两个map
   *
   * @return copy of this record
   */
  public Record copy() {
    Record record = new Record();
    record.setTableId(this.tableId);
    record.setTag(this.tag);
    record.setPkId(this.pkId);
    record.setRefInfo(this.refInfo.copy());

    return record;
  }

  @Override
  public Record clone() {
    Record record = new Record();
    record.setTableId(this.tableId);
    record.setTag(this.tag);
    record.setPkId(this.pkId);
    record.setRefInfo(this.refInfo.clone());
    record.lock = new ReentrantReadWriteLock();
    return record;
  }
}
