package trace;

import ana.main.Config;
import java.io.Serializable;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

/**
 * 代表一个operation的最小读写单元 即一个object，关系型数据库中为一个tuple table、primarykey、valueSet三者必须都不为null
 *
 * @author like_
 */
@Data
@Builder
public class TupleTrace implements Serializable {

  private static final long serialVersionUID = 1L;

  /** 访问的表 */
  private final String table;

  /** 访问的主键 如果访问的主键为null，那么认为该TupleTrace是一个加表锁的操作 */
  private final String primaryKey;

  /** 保存读写单元的读写结果 key为attributeName value为对应的读写结果 */
  private final Map<String, String> valueMap;

  /** debug信息：保存valueMap对应的真实值 */
  private Map<String, String> realValueMap;

  public TupleTrace(
      String table,
      String primaryKey,
      Map<String, String> valueMap,
      Map<String, String> realValueMap) {
    super();
    this.table = table;
    this.primaryKey = primaryKey;
    this.valueMap = valueMap;
    this.realValueMap = realValueMap;
  }

  /**
   * ！！只有验证端代码调用 获取TupleTrace.valueMap Config.TAKE_VC=true，利用虚拟列封装valueMap
   * Config.TAKE_VC=false，利用真实列封装valueMap
   *
   * @return
   */
  public Map<String, String> getValueMap() {
    if (Config.TAKE_VC) {
      return valueMap;
    } else {
      // return AnalysisThread.getMiniatureShadow().getRealValueMap(Integer.parseInt(table),
      // valueMap);
      return null;
    }
  }

  public String getKey() {
    return table + LINKER + primaryKey;
  }

  public void clearDebugInfo() {
    this.setRealValueMap(null);
  }

  public static final String LINKER = ",";
}
