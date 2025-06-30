package gen.shadow;

import gen.data.param.*;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;
import lombok.Data;

@Data
public class TableMirror implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  // 此表最大数目，也就是逻辑主键上限
  private int maxSize;

  // 此表所使用的的分区算法
  private PKPartitionAlg pkPartitionAlg;

  // 此表所使用的的主键生成参数(一列一套参数,pkId -> PKParam)
  private Map<Integer, PKParam> pkParamMap;

  // 动态区Record列表 (pkId -> Record)
  public transient Map<Integer, Record> dynamicRecordMap =
      Collections.synchronizedMap(new LinkedHashMap<>());

  // 主键前缀外键生成参数
  private FKParam pk2fkParam;

  // 此表所使用的的外键生成参数(一列一套参数, ForeignKey.ForeignKeyId->FKParam)
  private Map<Integer, FKParam> fkParamMap;

  // 此表所使用的的attr生成参数(一列一套参数, attrId->AttrParam)
  private Map<Integer, AttrParam> attrParamMap;

  // attrFunc列表，具体放在那里合适还不清楚
  private Map<Integer, AttrFunc> attrFuncMap;

  private Map<Integer, AttrFunc> ukFuncMap;
  private Map<Integer, AttrParam> ukParamMap;

  private List<Integer> recentAccessData = new ArrayList<>();

  /**
   * 插入新数据，如果cache超过阈值（20），丢弃最旧的数据
   *
   * @param newPkId 新的数据的pkid int
   */
  public void addRecentAccessData(int newPkId) {
    if (!recentAccessData.contains(newPkId)) {
      recentAccessData.add(newPkId);
    }

    if (recentAccessData.size() > 20) {
      recentAccessData.remove(new Random().nextInt(20));
    }
  }

  /**
   * 获取主键列id对应的主键列生成参数
   *
   * @param pkAttrId pkAttrId
   * @return PKParam
   */
  public PKParam getPkParamById(int pkAttrId) {
    return pkParamMap.get(pkAttrId);
  }

  /**
   * 获取外键id(ForeignKey.fkId)对应的外键生成参数
   *
   * @param fkId ForeignKey.fkId
   * @return FKParam
   */
  public FKParam getFkParamById(int fkId) {
    return fkParamMap.get(fkId);
  }

  /**
   * 获取Attribute对应的生成参数
   *
   * @param attrId Attribute.attrId
   * @return AttrParam
   */
  public AttrParam getAttrParamById(int attrId) {
    return attrParamMap.get(attrId);
  }

  public AttrParam getUKParamById(int ukId) {
    return ukParamMap.get(ukId);
  }

  /**
   * 获取Attribute对应的AttrFunc
   *
   * @param attrId Attribute.attrId
   * @return AttrFunc
   */
  public AttrFunc getAttrFuncById(int attrId) {
    return attrFuncMap.get(attrId);
  }

  public AttrFunc getUKFuncById(int ukId) {
    return ukFuncMap.get(ukId);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    // invoke default serialization method
    out.defaultWriteObject();
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    // invoke default serialization method
    in.defaultReadObject();
    dynamicRecordMap = new LinkedHashMap<>();
  }

  @Override
  public TableMirror clone() {
    TableMirror clone = new TableMirror();
    clone.maxSize = this.maxSize;
    clone.pkPartitionAlg = this.pkPartitionAlg;
    clone.pkParamMap = this.pkParamMap;
    clone.pk2fkParam = this.pk2fkParam;
    clone.fkParamMap = this.fkParamMap;
    clone.attrFuncMap = this.attrFuncMap;
    clone.ukFuncMap = this.ukFuncMap;
    clone.attrParamMap = this.attrParamMap;
    clone.ukParamMap = this.ukParamMap;
    clone.dynamicRecordMap = Collections.synchronizedMap(new LinkedHashMap<>());
    for (Integer key : this.dynamicRecordMap.keySet()) {
      clone.dynamicRecordMap.put(key, this.dynamicRecordMap.get(key).clone());
    }
    return clone;
  }

  /** @return 最近（20）访问过的一个pkId，可能为空 */
  public Integer getRandomRecentAccessData() {
    int idx = new Random().nextInt(20);
    if (recentAccessData.size() <= idx) {
      return null;
    }
    return recentAccessData.get(idx);
  }
}
