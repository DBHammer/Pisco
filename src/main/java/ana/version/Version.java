package ana.version;

import static ana.main.OrcaVerify.logger;

import ana.graph.Dependency;
import ana.main.Config;
import ana.main.OrcaVerify;
import ana.window.profile.ProfileMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import trace.IsolationLevel;
import trace.OperationTraceType;

/**
 * 封装一个写操作产生的包含时间区间信息未提交的新版本 封装一个提交操作产生的包含时间区间信息的提交版本 封装初始数据生成器产生的提交版本 封装一个回滚操作产生的包含时间区间信息的回滚版本
 * 封装一个时间区间的伪版本 封装一个不包含时间区间信息的未提交版本
 *
 * @author like_
 */
public class Version {

  /** 表示由哪个事务产生该version的 */
  @Getter private String transactionID;

  /**
   * 保存该版本的时间区间 如果该版本status处于COMMITTED，那么该时间区间为产生这个版本的事务提交的时间区间
   * 如果该版本status处于UNCOMMITTED，那么该时间区间为产生这个版本的操作的时间区间
   */
  @Getter private long startTimestamp;

  @Getter private long finishTimestamp;

  /** 保存读写单元的读写结果 key为attributeID value为对应的读写结果 */
  @Getter private Map<String, String> valueSet;

  /** 存放该版本的状态 */
  @Getter private VersionStatus status;

  /** 产生该版本的写操作 */
  @Getter private String producerID;

  /** 产生该版本的写操作类型 */
  @Setter @Getter private OperationTraceType producerType;

  @Getter private String traceFile;

  @Getter private String sql;

  /** 保存读取该版本的所有读操作所在的事务,只考虑已提交事务 */
  @Getter private ArrayList<String> readList;

  /** 记录该已提交Version是否考察过与其前驱节点构成WW依赖 */
  private boolean isWW;

  /** 在可以明确后继Version的情况下，记录该已提交Version的后继已提交Version，否则为null */
  @Setter private String successor;

  // 从csv文件中读出来的初始数据的cache: tableId -> pkId -> values(未解析string)
  private static Map<Integer, Map<Integer, String>> initialVersionCache = new HashMap<>();

  /**
   * 服务于读一致性时间区间的伪version
   *
   * @param startTimestamp timestamp of start
   * @param finishTimestamp timestamp of finish
   */
  public Version(long startTimestamp, long finishTimestamp) {
    super();
    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
  }

  /**
   * 创建数据的一个版本，包括两类，第一类是初始版本，第二类是负载动态生成过程中的未提交版本
   *
   * @param startTimestamp timestamp of start
   * @param finishTimestamp timestamp of finish
   * @param valueSet data set of version
   */
  public Version(
      String transactionID,
      long startTimestamp,
      long finishTimestamp,
      Map<String, String> valueSet,
      String producerID,
      OperationTraceType producerType,
      String traceFile,
      String sql) {
    super();
    this.transactionID = transactionID;

    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
    this.valueSet = valueSet;

    this.status = VersionStatus.UNCOMMITTED; // version的初始状态一律设置为UNCOMMITTED

    this.producerID = producerID;
    this.producerType = producerType;
    this.traceFile = traceFile;
    this.sql = sql;

    OrcaVerify.numberStatistic.increaseVersion(0);
  }

  /**
   * 基于一个未提交Version创建一个最终Version，即提交版本和回滚版本，并设置Version的时间区间、数据和状态
   *
   * @param startTimestamp timestamp of start
   * @param finishTimestamp timestamp of finish
   * @param unCommittedVersion version uncommitted
   * @param versionStatus status of version
   */
  public Version(
      long startTimestamp,
      long finishTimestamp,
      Version unCommittedVersion,
      VersionStatus versionStatus) {
    // 1.复制unCommittedVersion的相关数据
    this.transactionID = unCommittedVersion.getTransactionID();
    this.valueSet = unCommittedVersion.getValueSet();
    this.producerID = unCommittedVersion.getProducerID();
    this.producerType = unCommittedVersion.getProducerType();
    this.traceFile = unCommittedVersion.getTraceFile();
    this.sql = unCommittedVersion.getSql();

    // 2.重新设置最终Version的相关数据
    this.startTimestamp = startTimestamp;
    this.finishTimestamp = finishTimestamp;
    this.status = versionStatus;

    // 3.服务于构建WR依赖，排除初始版本和回滚事务
    if (!this.transactionID.equals("-1,-1") && this.status == VersionStatus.COMMITTED) {
      this.readList = new ArrayList<>();
    }

    // 4.统计
    switch (this.status) {
      case COMMITTED:
        OrcaVerify.numberStatistic.increaseVersion(1);
        break;
      case ROLLBACK:
        OrcaVerify.numberStatistic.increaseVersion(2);
        break;
    }
  }

  /**
   * 服务于write set(read consistency)的构造函数 时间对于创建write set(read consistency)中的version不必要，可以忽略
   *
   * @param valueSet write data set
   */
  public Version(
      Map<String, String> valueSet,
      String producerID,
      OperationTraceType producerType,
      String traceFile,
      String sql) {
    super();
    this.valueSet = valueSet;

    this.producerID = producerID;
    this.producerType = producerType;
    this.traceFile = traceFile;
    this.sql = sql;
  }

  /**
   * 检查valueSet是否与this.valueSet兼容，即valueSet中与this.valueSet相同key的值是否相同，忽略那些在this.valueSet和valueSet中没出现的key
   *
   * @param valueSet value set checked
   * @return two value set is compatible
   */
  public boolean isCompatible(Map<String, String> valueSet) {

    String value;
    for (Map.Entry<String, String> entry : this.valueSet.entrySet()) {
      String key = entry.getKey();
      if ((value = valueSet.get(key)) != null) {
        try {
          double valueNum = Double.parseDouble(value);
          double enterNum = Double.parseDouble(entry.getValue());
          return valueNum == enterNum;
        } catch (NumberFormatException e) {
          return value.equals(entry.getValue());
        }
      }
    }
    // 新版本和旧版本的存在性应该是一致的，也就是要不都是空集、要不都不是空集
    boolean thisEmpty = this.valueSet.isEmpty();
    boolean newEmpty = valueSet.isEmpty();
    return (thisEmpty && newEmpty) || (!thisEmpty && !newEmpty);
  }

  /**
   * 为版本添加新的reader，如果reader已经存在不再重复添加，且reader必须是已提交事务且不是初始版本
   *
   * @param newReaderID reader id
   * @param dependencyKey key of dependency
   * @param readOperationTraceID operation trace id
   */
  public void addNewReader(String newReaderID, String dependencyKey, String readOperationTraceID) {
    // 1.排除当前版本为初始版本
    if (transactionID.equals("-1,-1")
        ||
        // 排除读者为不是提交的事务
        ProfileMap.getProfile(newReaderID).getEndType() != OperationTraceType.COMMIT
        ||
        // 排除读者为读未提交隔离级别的事务
        ProfileMap.getProfile(newReaderID).getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
      return;
    }

    // 2.排除重复
    for (String reader : this.readList) {
      if (reader.equals(newReaderID)) {
        return;
      }
    }

    // 3.成功添加，并构建依赖在依赖图中
    this.readList.add(newReaderID);
    Dependency.trackWR(
        transactionID, newReaderID, successor, dependencyKey, producerID, readOperationTraceID);
  }

  public boolean isWW() {
    return isWW;
  }

  public void setWW(boolean WW) {
    isWW = WW;
  }

  /**
   * 判断两个version的时间区间是否重叠 <a href="https://www.cnblogs.com/liuwt365/p/7222549.html">...</a>
   *
   * @return time period overlapped
   */
  public static boolean isOverlapping(Version version1, Version version2) {
    // max(start) > min(finish),则两个区间不相交
    return Math.max(version1.getStartTimestamp(), version2.getStartTimestamp())
        <= Math.min(version1.getFinishTimestamp(), version2.getFinishTimestamp());
  }

  /**
   * 根据表和主键值，即数据的标识，获取初始版本 CALCULATE_INITIAL_DATA，决定使用IO的方式或者计算的方式生成初始版本
   * TAKE_VC，决定以虚拟列还是真实列的方式维护初始版本
   *
   * @param tableId table id
   * @param pkId primary key id
   * @return initial data version
   */
  public static Map<String, String> getInitialVersionValueSet(int tableId, int pkId) {
    long startTS = System.nanoTime();

    // 如果没有对应表的cache，new 一个
    initialVersionCache.computeIfAbsent(tableId, k -> new HashMap<>());

    Map<String, String> initialVersionValueSet = null;

    // 1.通过配置项CALCULATE_INITIAL_DATA决定以计算的方式还是以IO的方式生成初始数据
    if (!Config.CALCULATE_INITIAL_DATA) {

      // 1.2通过IO获取初始数据
      BufferedReader br = null;
      try {
        br =
            new BufferedReader(
                new InputStreamReader(
                    Files.newInputStream(
                        Paths.get(
                            Config.ANALYSIS_TARGET_DIR + "/init_data/table" + tableId + ".csv"))));

        String recordLine;

        // 1.2.1获取当前csv文件的表头信息
        String tableHeader = br.readLine();
        String[] keys = tableHeader.trim().split(",");
        // 数据清洗
        keys = Arrays.stream(keys).map(key -> StringUtils.strip(key, "\"'")).toArray(String[]::new);

        if ((recordLine = initialVersionCache.get(tableId).get(pkId)) == null) { // 如果cache里没存
          // 1.2.2逐行检查读取的recordLine是否是指定pkID的初始数据
          while ((recordLine = br.readLine()) != null) {
            int firstCommaIndex = recordLine.indexOf(',');
            String firstElement =
                (firstCommaIndex == -1) ? recordLine : recordLine.substring(0, firstCommaIndex);
            firstElement = StringUtils.strip(firstElement, "\"'");
            int dataId = Integer.parseInt(firstElement);
            // 1.2.2.1指定pkID的所在recordLine的充要条件
            if (dataId == pkId) {
              break;
            } else {
              initialVersionCache.get(tableId).putIfAbsent(dataId, recordLine);
            }
          }
        }
        if (recordLine == null) {
          return null;
        }

        String[] values = recordLine.split(",");
        for (int i = 1; i < values.length; i++) {
          values[i] = StringUtils.strip(values[i], "\"'");
        }
        initialVersionCache.get(tableId).remove(pkId);
        // 1.2.2.2解析recordLine获取其代表的逻辑值valueMap
        Map<String, String> valueMap = new HashMap<>();

        for (int ind = 1; ind < keys.length; ind++) {
          valueMap.put(keys[ind], values[ind]);
        }

        initialVersionValueSet = valueMap;
      } catch (IOException e) {
        logger.warn(e);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (IOException e) {
            logger.warn(e);
          }
        }
      }
    }
    long finishTS = System.nanoTime();
    OrcaVerify.runtimeStatistic.increaseGetInitialVersion(finishTS - startTS);

    // 2.通过配置项TAKE_VC决定以虚拟列方式维护初始数据还是以真实列方式维护初始数据
    if (initialVersionValueSet != null) {
      // 2.1虚拟列方式：直接返回
      if (Config.TAKE_VC) {
        return initialVersionValueSet;
      }

      // 2.12真实列方式：调用转换函数，转换成真实列
      return initialVersionValueSet;
    }

    // 3.生成初始数据失败，返回null
    return null;
  }
}
