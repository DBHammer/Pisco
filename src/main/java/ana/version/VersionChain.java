package ana.version;

import ana.main.OrcaVerify;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import trace.TupleTrace;

/**
 * 封装一个数据所有的version,按照version的结束时间有序排序
 *
 * @author like_
 */
public class VersionChain {

  /** 核心数据结构 将所有Version按照结束时间有序存放(逆序)，结束时间大的放在前面 */
  private final LinkedList<Version> versionChain;

  public VersionChain(String key) {
    super();
    this.versionChain = new LinkedList<>();

    // 1.将key拆分成tableID、pK ID
    String[] t = key.split(TupleTrace.LINKER);
    int tableId = Integer.parseInt(t[0].substring(5 + t[0].indexOf("table")));
    int pkId = Integer.parseInt(t[1]);

    // 2.默认生成一个初始版本添加到VersionChain
    Map<String, String> initialVersionValueSet = Version.getInitialVersionValueSet(tableId, pkId);
    // 2.1如果initialVersionValueSet为null，说明该数据没有初始版本，则versionChain中不需要添加初始version
    if (initialVersionValueSet != null) {
      // 2.2设置初始版本为已提交状态，时间为系统最小时间,创建的事务设置为-1,-1
      Version initialVersion =
          new Version(
              Long.MIN_VALUE,
              Long.MIN_VALUE + 1,
              new Version(
                  "-1,-1",
                  Long.MIN_VALUE,
                  Long.MIN_VALUE + 1,
                  initialVersionValueSet,
                  "-1,-1,-1",
                  null,
                  "initial version",
                  "initial version"),
              VersionStatus.COMMITTED);
      this.versionChain.add(initialVersion);

      // 统计
      OrcaVerify.numberStatistic.increaseVersion(3);
    }
  }

  /**
   * 按结束时间有序将Version添加到versionChain中
   *
   * @param newVersion new version that is older than all in version chain
   */
  public void addVersion(Version newVersion) {
    ListIterator<Version> listIterator = versionChain.listIterator();

    Version nextVersion;
    while (listIterator.hasNext()) {
      nextVersion = listIterator.next();

      if (nextVersion.getFinishTimestamp() < newVersion.getFinishTimestamp()) {
        listIterator.previous();
        break;
      }
    }
    listIterator.add(newVersion);
    return;
  }

  /**
   * 返回versionChain一个全新的迭代器
   *
   * @return
   */
  public ListIterator<Version> getIterator() {
    return this.versionChain.listIterator();
  }

  /**
   * 返回versionChain的长度
   *
   * @return
   */
  public int getLength() {
    return this.versionChain.size();
  }

  /**
   * 给定版本链中某一个已提交Version，检查该Version是否与其他已提交Version重叠
   *
   * @param version
   * @return
   */
  public boolean isOverlapping(Version version) {
    ListIterator<Version> listIterator = getIterator();
    Version nextVersion;

    // 1.对VersionChain中每一个已提交Version，检查是否与目标Version重叠
    while (VersionChain.hasNext(listIterator, true)) {
      nextVersion = listIterator.next();
      if (Version.isOverlapping(nextVersion, version)) {
        return true;
      }
      // 优化：如果VersionChain中某个版本的结束时间戳小于目标version的开始时间戳，那么VersionChain之后的所有version都不可能与目标version重叠
      if (version.getStartTimestamp() >= nextVersion.getFinishTimestamp()) {
        return false;
      }
    }
    return false;
  }

  /**
   * 处理是否需要忽略未提交version的细节，回滚version一定忽略
   *
   * @param listIterator
   * @param isIgnoreUncommitted
   * @return
   */
  public static boolean hasNext(ListIterator<Version> listIterator, boolean isIgnoreUncommitted) {
    Version nextVersion = null;

    // 1.一直迭代listIterator，
    // 直到达到listIterator的末尾，为空就不用跳过下一个version
    // 或者第一个状态不为回滚的version（isIgnoreUncommitted==false），找到一个不为回滚version的version就不用跳过下一个version
    // 或者找到第一个状态已提交的version（isIgnoreUncommitted==true），找到一个已提交version就不用跳过下一个version
    while (listIterator.hasNext()
        && ((nextVersion = listIterator.next()).getStatus() == VersionStatus.ROLLBACK
            || isIgnoreUncommitted && nextVersion.getStatus() == VersionStatus.UNCOMMITTED)) {
      // 跳过一个version
      nextVersion = null;
    }

    // 2.nextVersion == null说明到达listIterator的末尾也没有找到状态不为未提交的version
    if (nextVersion == null) {
      return false;
    }

    // 3.说明找到了一个状态为已提交version（isIgnoreUncommitted==true）
    // 或者未提交或者已提交version(isIgnoreUncommitted==false)
    // 将listIterator的cursor回退一个
    listIterator.previous();
    return true;
  }
}
