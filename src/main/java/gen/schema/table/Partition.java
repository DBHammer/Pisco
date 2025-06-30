package gen.schema.table;

import static context.OrcaContext.configColl;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import context.OrcaContext;
import gen.data.type.DataType;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Data;
import util.rand.RandUtil;

@Data
public class Partition implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String tableName;

  private final Attribute partitionKey;
  private final String dbName;
  private PartitionType partitionType;

  // id可能变化，idx才是有意义的
  private final List<Integer> partitionIds;

  private final List<Integer> partitionRanges = new CopyOnWriteArrayList<>();

  private final List<String> nodeList = new CopyOnWriteArrayList<>();
  // rg -> list of nodes
  private final Map<String, List<String>> rgNodes = new HashMap<>();
  // rg -> its leader node
  private final Map<String, String> rgLeaderNode = new HashMap<>();
  // rg -> its region
  private final Map<String, List<String>> region = new ConcurrentHashMap<>();
  // partition -> region
  private final Map<String, String> partition2Region = new HashMap<>();
  // 近期修改的region/rg
  private final Queue<String> recentChangedData = new LinkedList<>();

  /** 当前attribute索引的两种状态 true:有索引 false:没有索引 */
  private boolean secondaryIndex;

  public Partition(List<Attribute> pk, String tableName) {
    super();

    this.tableName = tableName;

    String url = configColl.getDatasource().getUrl();
    this.dbName = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));

    RandUtil randUtil = new RandUtil();

    // 生成partition key
    int partitionKeyIdx = randUtil.nextInt(pk.size());
    this.partitionKey = pk.get(partitionKeyIdx);

    if (!partitionKey.getAttrType().equals(DataType.INTEGER)) {
      this.partitionType = PartitionType.KEY;
    } else {
      this.partitionType = PartitionType.values()[randUtil.nextInt(PartitionType.values().length)];
    }

    // 生成具体的分区
    int recordNumberLimit =
        OrcaContext.configColl.getSchema().getTable().getRecordNumber().getEnd();
    int partitionSize = randUtil.nextInt(recordNumberLimit) / 100 + 10;
    partitionSize = Math.min(partitionSize, 10000);

    this.partitionIds = new CopyOnWriteArrayList<>();
    for (int i = 0; i < partitionSize; i++) {
      partitionIds.add(i);
    }

    int startValue = 0;
    // pk 生成时每一步的步长
    int valueStep = configColl.getDataGenerate().getPkParamConfig().getStep().getEnd();
    // partition每个分区的范围
    int step = (recordNumberLimit / partitionSize + partitionSize) * valueStep;
    if (partitionType.equals(PartitionType.RANGE)) {
      for (int i = 0; i < partitionSize; i++) {
        startValue += step;
        partitionRanges.add(startValue);
      }
    }
  }

  public String getNonLeaderId(String rgId) {
    RandUtil randUtil = new RandUtil();

    List<String> nodes = rgNodes.get(rgId);

    if (nodes.size() <= 1) {
      throw new RuntimeException("node size not enough: less than 2");
    }

    String leader = rgLeaderNode.get(rgId);
    String newLeader = nodes.get(randUtil.nextInt(nodes.size()));
    while (newLeader.equals(leader)) {
      newLeader = nodes.get(randUtil.nextInt(nodes.size()));
    }
    return newLeader;
  }

  public List<String> getRegion(String rgId) {
    return region.get(rgId);
  }

  public String getAnotherRgId(String rgId) {
    RandUtil randUtil = new RandUtil();

    List<String> rgIds = new ArrayList<>(rgNodes.keySet());

    if (rgIds.size() <= 1) {
      throw new RuntimeException("rg not enough: less than 2");
    }

    String newRgId = rgIds.get(randUtil.nextInt(rgIds.size()));
    while (newRgId.equals(rgId)) {
      newRgId = rgIds.get(randUtil.nextInt(rgIds.size()));
    }
    return newRgId;
  }

  public int getPartitionSize() {
    return partitionIds.size();
  }

  public String toString() {
    StringBuilder partitionSQL = new StringBuilder();
    String partitionBy =
        String.format(" PARTITION BY %s(%s) ", partitionType.name(), partitionKey.getAttrName());
    partitionSQL.append(partitionBy);
    int partitionSize = partitionIds.size();
    switch (partitionType) {
      case RANGE:
        List<String> ranges = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
          String partitionName = String.format("p%s", partitionIds.get(i));
          String rangeValue = String.valueOf(partitionRanges.get(i));
          ranges.add(
              String.format(" PARTITION %s VALUES LESS THAN (%s) ", partitionName, rangeValue));
        }
        partitionSQL.append(String.format(" ( %s ) ", String.join(",", ranges)));
        break;
      case HASH:
      case KEY:
        partitionSQL.append(String.format(" PARTITIONS %s ", partitionSize));
        break;
    }
    return partitionSQL.toString();
  }

  public synchronized void getPhysicalPartition(Connection conn) throws SQLException {

    // get rg info
    String rgQuery =
        "select rep_group_id, leader_node_name, member_node_names from information_schema.meta_cluster_rgs;";
    /*
    +--------------+------------------+------------------------------------------+
    | rep_group_id | leader_node_name | member_node_names                        |
    +--------------+------------------+------------------------------------------+
    |      7680768 | node-9-002       | ["node-9-001","node-9-002","node-9-003"] |
    +--------------+------------------+------------------------------------------+
     */
    Statement stat = conn.createStatement();

    ResultSet rs = stat.executeQuery(rgQuery);

    Gson gson = new Gson();
    Type type = new TypeToken<List<String>>() {}.getType();

    rgNodes.clear();
    rgLeaderNode.clear();
    region.clear();
    nodeList.clear();
    partition2Region.clear();

    while (rs.next()) {
      long rgId = rs.getLong(1);
      String leader = rs.getString(2);
      rgLeaderNode.put(String.valueOf(rgId), leader);

      String rg = rs.getString(3);
      List<String> nodes = gson.fromJson(rg, type);
      rgNodes.put(String.valueOf(rgId), nodes);
    }

    // get rg info
    String regionQuery =
        String.format(
            "select data.data_obj_name,region.region_id, region.rep_group_id "
                + "from information_schema.META_CLUSTER_DATA_OBJECTS as data,information_schema.meta_cluster_regions as region "
                + "where data.schema_name='%s' and data.table_name='%s' and data.data_obj_id=region.data_obj_id order by region.start_key;",
            dbName, tableName);
    /*
    +---------------+-----------+--------------+
    | data_obj_name | region_id | rep_group_id |
    +---------------+-----------+--------------+
    | t.p8          |    377182 |     96993919 |
    | t.p5          |    377181 |     96993919 |
     */
    stat = conn.createStatement();
    rs = stat.executeQuery(regionQuery);

    while (rs.next()) {

      long regionId = rs.getLong(2);
      long rgId = rs.getLong(3);

      String dataObjName = rs.getString(1);
      if (dataObjName.contains(".")) {
        int idx = dataObjName.indexOf(".");
        String partition = dataObjName.substring(idx + 1);
        partition2Region.put(partition, String.valueOf(regionId));
      } else {
        partition2Region.put(dataObjName, String.valueOf(regionId));
      }

      if (!region.containsKey(String.valueOf(rgId))) {
        region.put(String.valueOf(rgId), new ArrayList<>());
      }
      region.get(String.valueOf(rgId)).add(String.valueOf(regionId));
    }
  }

  public synchronized void updateChangedInfo(String obj) {
    recentChangedData.add(obj);
    if (recentChangedData.size() > 5) {
      recentChangedData.remove();
    }
  }

  public String getRG(int key) {
    if (key == -1) {
      return getRG();
    }
    return getRGByKey(key);
  }

  public String getRG() {
    if (region.isEmpty()) {
      return null;
    }

    RandUtil randUtil = new RandUtil();
    List<String> rgIds = new ArrayList<>(region.keySet());
    return rgIds.get(randUtil.nextInt(rgIds.size()));
  }

  public String getRegionByKey(int key) {
    return partition2Region.get(getPartitionByKey(key));
  }

  public String getRGByKey(int key) {
    String regionId = getRegionByKey(key);
    for (String rg : region.keySet()) {
      if (region.get(rg).contains(regionId)) {
        return rg;
      }
    }
    return null;
  }

  public String getPartitionByKey(int key) {
    int idx = key % getPartitionSize();
    return String.format("p%d", partitionIds.get(idx));
  }

  public boolean keyInRecentChangedData(int key) {
    String regionId = getRegionByKey(key);
    String rgId = getRGByKey(key);
    return recentChangedData.contains(regionId) || recentChangedData.contains(rgId);
  }

  private String getPartitionByRegion(String region) {
    for (String partition : partition2Region.keySet()) {
      if (partition2Region.get(partition).equals(region)) {
        return partition;
      }
    }
    return null;
  }

  public String getAccessPartition(String obj) {
    List<String> partitions = new ArrayList<>();
    for (String rg : region.keySet()) {
      if (rg.equals(obj)) {
        for (String regionId : region.get(rg)) {
          partitions.add(getPartitionByRegion(regionId));
        }
        break;
      }
      if (region.get(rg).contains(obj)) {
        partitions.add(getPartitionByRegion(obj));
        break;
      }
    }
    return String.join(",", partitions);
  }
}
