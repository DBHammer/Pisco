package gen.shadow;

import adapter.Adapter;
import config.data.DataGenConfig;
import config.data.DataRepoConfig;
import config.data.PartitionAlgoConfig;
import config.schema.DistributionType;
import gen.data.InitialData;
import gen.data.generator.AttrGenerator;
import gen.data.generator.FKGenerator;
import gen.data.generator.PKGenerator;
import gen.data.generator.UKGenerator;
import gen.data.param.*;
import gen.data.param.func.FixedPartitionAlg;
import gen.data.param.func.HashPartitionAlg;
import gen.data.param.func.SimpleAttrFunc;
import gen.data.param.func.SimplePKFunc;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import gen.operation.param.ParamInfo;
import gen.schema.Schema;
import gen.schema.generic.AbstractTable;
import gen.schema.table.Attribute;
import gen.schema.table.ForeignKey;
import gen.schema.table.Table;
import gen.schema.view.View;
import io.Storable;
import java.util.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import symbol.Symbol;
import util.access.distribution.SampleDistribution;
import util.rand.RandUtils;
import util.xml.Seed;

public class DBMirror extends Storable {
  final Logger logger = LogManager.getLogger(this.getClass());
  private final Schema schema;
  // tableId -> TableGenModel
  private final Map<Integer, TableMirror> tableMirrorMap;
  private final DataRepo dataRepo;
  private final Random random = new Random();

  // backup & restore
  private final Map<Integer, TableMirror> tableMirrorMapBackup;

  /** 根据schema进行初始化生成tableGenModelList */
  public DBMirror(Schema schema, DataGenConfig dataGenConfig) {
    super();
    this.schema = schema;
    this.tableMirrorMap = Collections.synchronizedMap(new LinkedHashMap<>());
    DataRepoConfig dataRepoConfig = dataGenConfig.getDataRepoConfig();
    int dataUnique = dataRepoConfig.getDataUnique();
    //        logger.info("dataUnique: " + dataUnique);
    this.dataRepo = new DataRepo(dataRepoConfig, dataUnique);

    this.tableMirrorMapBackup = Collections.synchronizedMap(new LinkedHashMap<>());

    this.initParams(dataGenConfig);
  }

  public DBMirror(MirrorData mirrorData) {
    this.schema = mirrorData.getSchema();
    this.tableMirrorMap = mirrorData.getTableMirrorMap();
    this.dataRepo = mirrorData.getDataRepo();

    this.tableMirrorMapBackup = Collections.synchronizedMap(new LinkedHashMap<>());
  }

  public MirrorData getMirrorData() {
    return new MirrorData(this.schema, this.tableMirrorMap, this.dataRepo);
  }

  /** 配置生成参数 */
  private void initParams(DataGenConfig dataGenConfig) {
    for (Table table : schema.getTableList()) {

      TableMirror tableMirror = new TableMirror();

      // 设置表长度限制，即文档中的N，逻辑主键限制
      tableMirror.setMaxSize(table.getTableSize());

      // 设置此表的分区算法
      PartitionAlgoConfig partitionAlgoConfig = dataGenConfig.getPartitionAlgoConfig();

      PKPartitionAlg pkPartitionAlg;
      if (dataGenConfig.getPartitionStrategy() == PartitionStrategy.HASH) {
        pkPartitionAlg =
            new HashPartitionAlg(
                partitionAlgoConfig.getProbOfStatic(),
                partitionAlgoConfig.getProbOfDynamicExists(),
                random.nextInt());
      } else if (dataGenConfig.getPartitionStrategy() == PartitionStrategy.FIXED) {
        pkPartitionAlg =
            new FixedPartitionAlg(
                partitionAlgoConfig.getProbOfStatic(),
                partitionAlgoConfig.getProbOfDynamicExists(),
                table.getTableSize());
      } else {
        throw new RuntimeException(
            "Unsupported Partition Strategy: " + dataGenConfig.getPartitionStrategy());
      }

      tableMirror.setPkPartitionAlg(pkPartitionAlg);

      // 为主键的每一列添加生成参数
      Map<Integer, PKParam> pkParamMap = new HashMap<>();
      for (Attribute pkAttr : table.getPrimaryKey()) {
        //                SampleDistribution stepDistribution =
        //                        SampleDistribution.getAccessDistribution();
        Seed stepDistribution = dataGenConfig.getPkParamConfig().getStep();
        PKParam pkParam =
            new PKParam(stepDistribution.getRandomValue(), new SimplePKFunc(random.nextInt()));
        pkParamMap.put(pkAttr.getAttrId(), pkParam);
      }
      tableMirror.setPkParamMap(Collections.synchronizedMap(pkParamMap));

      // 主键前缀外键生成参数
      if (table.getPk2ForeignKey() != null) {
        DistributionType pk2fkDistType =
            RandUtils.randSelectByProbability(dataGenConfig.getFkParamConfig().getDistMap());
        SampleDistribution pk2fkDist =
            SampleDistribution.getAccessDistribution(
                pk2fkDistType,
                0,
                table.getPk2ForeignKey().getReferencedTable().getTableSize(),
                null);
        tableMirror.setPk2fkParam(new FKParam(pk2fkDist));
      }

      // 为每一个普通外键添加生成参数
      Map<Integer, FKParam> fkParamMap = new HashMap<>();
      for (ForeignKey fk : table.getCommonForeignKeyList()) {
        // 主键前缀外键生成参数
        DistributionType fkDistType =
            RandUtils.randSelectByProbability(dataGenConfig.getFkParamConfig().getDistMap());
        SampleDistribution fkDist =
            SampleDistribution.getAccessDistribution(
                fkDistType, 0, fk.getReferencedTable().getTableSize(), null);
        FKParam fkParam = new FKParam(fkDist);
        fkParamMap.put(fk.getId(), fkParam);
      }
      tableMirror.setFkParamMap(Collections.synchronizedMap(fkParamMap));

      // 为每一个Attribute设置生成参数和FuncList
      Map<Integer, AttrParam> attrParamMap = new HashMap<>();
      Map<Integer, AttrFunc> attrFuncMap = new HashMap<>();
      for (Attribute attr : table.getAttributeList()) {
        DistributionType attrDistType =
            RandUtils.randSelectByProbability(dataGenConfig.getAttrParamConfig().getDistMap());
        SampleDistribution attrDist =
            SampleDistribution.getAccessDistribution(
                attrDistType,
                0,
                10000000, // todo 此处的范围改如何定义
                null);
        AttrParam attrParam = new AttrParam(attrDist);
        attrParamMap.put(attr.getAttrId(), attrParam);
        attrFuncMap.put(attr.getAttrId(), new SimpleAttrFunc(random.nextInt()));
      }

      tableMirror.setAttrParamMap(Collections.synchronizedMap(attrParamMap));
      tableMirror.setAttrFuncMap(Collections.synchronizedMap(attrFuncMap));

      Map<Integer, AttrParam> ukParamMap = new HashMap<>();
      Map<Integer, AttrFunc> ukFuncMap = new HashMap<>();
      for (Attribute attr : table.getUniqueKey()) {
        DistributionType attrDistType =
            RandUtils.randSelectByProbability(dataGenConfig.getUkParamConfig().getDistMap());
        SampleDistribution attrDist =
            SampleDistribution.getAccessDistribution(
                attrDistType,
                0,
                10000000, // todo 此处的范围改如何定义
                null);
        AttrParam attrParam = new AttrParam(attrDist);
        ukParamMap.put(attr.getAttrId(), attrParam);
        ukFuncMap.put(attr.getAttrId(), new SimpleAttrFunc(random.nextInt()));
      }
      tableMirror.setUkParamMap(Collections.synchronizedMap(ukParamMap));
      tableMirror.setUkFuncMap(Collections.synchronizedMap(ukFuncMap));

      tableMirrorMap.put(table.getTableId(), tableMirror);
    }
  }

  /**
   * 由于表之间存在依赖关系，需要进行拓扑排序
   *
   * @param adapter adapter提供insert方法
   * @return List of insert SQL
   */
  public InitialData initData(Adapter adapter) throws InterruptedException {

    // 首先建立DynamicRecord结构
    schema.getTableList().forEach(this::initDynamicRecordForTable);

    assert tableMirrorMap.size() == schema.getTableList().size();
    List<String> insertSQLList = new ArrayList<>();

    Set<Table> unInitTableSet = new HashSet<>(schema.getTableList());
    List<Table> topologyOrderedTableList = topologyOrder(unInitTableSet);
    HashMap<Integer, List<String>> sqlListOfTables = new HashMap<>();
    //    List<Thread> subThreads = new ArrayList<>();
    //    CountDownLatch countDownLatch = new CountDownLatch(topologyOrderedTableList.size());
    for (Table table : topologyOrderedTableList) {
      //      Thread t =
      //          new Thread(
      //              () -> {
      //
      //              });
      //      subThreads.add(t);
      logger.info(String.format("Initiating table%d...", table.getTableId()));
      List<String> sqlList = initDataForTable(adapter, table);
      sqlListOfTables.put(table.getTableId(), sqlList);
      logger.info(String.format("Initiating table%d... FINISHED!!!", table.getTableId()));
      //      countDownLatch.countDown();
    }
    //    subThreads.forEach(Thread::start);
    //    countDownLatch.await();

    for (Table table : topologyOrderedTableList) {
      insertSQLList.addAll(sqlListOfTables.get(table.getTableId()));
    }

    // backup
    this.backup();

    return new InitialData(insertSQLList);
  }

  /**
   * 对表进行拓扑排序
   *
   * @param unInitTableSet tableSet
   * @return 按照拓扑序排列的TableList
   */
  public List<Table> topologyOrder(Set<Table> unInitTableSet) {
    List<Table> topologyOrderedTableList = new ArrayList<>();
    while (!unInitTableSet.isEmpty()) {
      Table canInitTable = findTopologySource(unInitTableSet.iterator().next(), unInitTableSet);
      topologyOrderedTableList.add(canInitTable);
      unInitTableSet.remove(canInitTable);
    }
    return topologyOrderedTableList;
  }

  /**
   * 找到下一个拓扑排序源头
   *
   * @param table 从哪张表开始
   * @param unInitTableSet 当前未初始化的表集合
   * @return 下一个可以进行初始化的表
   */
  public Table findTopologySource(Table table, Set<Table> unInitTableSet) {
    Set<Table> dependTableSet = table.getDependTableSet();

    // 求交集，因为目的是要找到下一个可以进行初始化的表
    // 求交集可以排除掉所依赖的表当中已经初始化过的表
    dependTableSet.retainAll(unInitTableSet);

    // 移除自身，可以简化下面的循环过程，自身依赖无需考虑
    dependTableSet.remove(table);

    for (Table depTable : dependTableSet) {
      // 递归进行此过程
      return findTopologySource(depTable, unInitTableSet);
    }

    // 如果仅仅依赖自身或者不依赖任何表
    // 则该表可以初始化
    return table;
  }

  /** 为某一张表建立DynamicRecord结构 */
  private void initDynamicRecordForTable(Table table) {
    TableMirror tableMirror = getTableMirrorById(table.getTableId());

    // 表的每一个pkId
    for (int pkId = 0; pkId < tableMirror.getMaxSize(); pkId++) {
      PartitionTag partitionTag = PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg());

      // 若该pkId属于动态区且不存在初始数据
      if (partitionTag == PartitionTag.DYNAMIC_NOT_EXISTS) {
        // 添加到 dynamicRecordMap 中
        tableMirror.dynamicRecordMap.put(
            pkId,
            new Record(table.getTableId(), pkId, PartitionTag.DYNAMIC_NOT_EXISTS, new RefInfo()));
      } else if (partitionTag == PartitionTag.DYNAMIC_EXISTS) { // 初始化数据
        // 添加到 dynamicRecordMap 中
        tableMirror.dynamicRecordMap.put(
            pkId, new Record(table.getTableId(), pkId, PartitionTag.DYNAMIC_EXISTS, new RefInfo()));
      }
    }
  }

  /**
   * 为某一张表生成初始数据
   *
   * @param adapter adapter提供insert方法
   * @return sqlList
   */
  private List<String> initDataForTable(Adapter adapter, Table table) {
    // todo lxr DYNAMIC_NOT_EXISTS时怎么办
    // 检测

    List<String> insertSQLList = new ArrayList<>();
    TableMirror tableMirror = getTableMirrorById(table.getTableId());

    // 表的每一个pkId
    for (int pkId = 0; pkId < tableMirror.getMaxSize(); pkId++) {
      PartitionTag partitionTag = PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg());

      // 若该pkId属于动态区且不存在初始数据
      if (partitionTag == PartitionTag.STATIC) {
        HashMap<String, AttrValue> col2val = initByPkId(table.getTableId(), pkId);
        assert col2val != null;
        insertSQLList.add(adapter.insert(table.getTableName(), col2val));
      } else if (partitionTag == PartitionTag.DYNAMIC_EXISTS) {
        // 引用信息，此参数将被传递给初始数据生成部分，由初始数据生成部分代码进行完善
        // 将该Record的RefInfo作为参数传递，以便填充
        RefInfo refInfo = tableMirror.dynamicRecordMap.get(pkId).getRefInfo();
        HashMap<String, AttrValue> col2val = initByPkId(table.getTableId(), pkId, refInfo);
        assert col2val != null;
        insertSQLList.add(adapter.insert(table.getTableName(), col2val));
      }
    }
    return insertSQLList;
  }

  /**
   * 获取某一列的初始值
   *
   * @param tableId tableId
   * @param pkId pkId
   * @return 对于静态区pkId和初始存在的动态区pkId，返回HashMap；对于动态区，返回null
   */
  public HashMap<String, AttrValue> initByPkId(int tableId, int pkId) {
    return initByPkId(tableId, pkId, null);
  }

  /**
   * 获取某一列的初始值
   *
   * @param tableId tableId
   * @param pkId pkId
   * @param refInfo 为存在初始值的动态区pkId构造时需要此参数
   * @return 对于静态区pkId和初始存在的动态区pkId，返回HashMap；对于动态区，返回null
   */
  public HashMap<String, AttrValue> initByPkId(int tableId, int pkId, RefInfo refInfo) {

    Table table = schema.findTableById(tableId);
    TableMirror tableMirror = getTableMirrorById(table.getTableId());

    PartitionTag partitionTag = PKGenerator.calcPartition(pkId, tableMirror.getPkPartitionAlg());

    // 若为初始不存在的动态区数据，返回null
    if (partitionTag == PartitionTag.DYNAMIC_NOT_EXISTS) {
      return null;
    }

    // 生成插入语句
    HashMap<String, AttrValue> col2val = new LinkedHashMap<>();

    // pkId
    col2val.put("pkId", new AttrValue(DataType.INTEGER, pkId));

    // 主键属性
    int nonFkPkAttrBegin = 0; // 第一个非外键主键属性
    for (int ind = nonFkPkAttrBegin; ind < table.getPrimaryKey().size(); ind++) {
      Attribute attr = table.getPrimaryKey().get(ind);
      col2val.put(
          attr.getAttrName(),
          PKGenerator.pkId2Pk(pkId, attr.getAttrType(), tableMirror.getPkParamById(ind)));
    }

    // 唯一键属性
    for (int i = 0; i < table.getUniqueKey().size(); i++) {
      Attribute attribute = table.getUniqueKey().get(i);
      col2val.put(
          attribute.getAttrName(),
          AttrGenerator.attrId2Attr(
              pkId,
              attribute.getAttrType(),
              tableMirror.getUkFuncMap().get(attribute.getAttrId()),
              this.dataRepo));
    }

    // Attribute (似乎不包括外键属性)
    for (Attribute attr : table.getAttributeList()) {
      col2val.put(
          attr.getAttrName(),
          AttrGenerator.attrId2Attr(
              AttrGenerator.staticAttrId(pkId, tableMirror.getAttrParamById(attr.getAttrId())),
              attr.getAttrType(),
              tableMirror.getAttrFuncMap().get(attr.getAttrId()),
              this.dataRepo));
    }

    // 普通外键属性
    for (ForeignKey fk : table.getCommonForeignKeyList()) {
      Table refToTable = fk.getReferencedTable();
      TableMirror refToTableMirror = getTableMirrorById(refToTable.getTableId());

      int fkId =
          FKGenerator.staticFkId(pkId, tableMirror.getFkParamById(fk.getId()))
              % refToTableMirror.getMaxSize();

      if (partitionTag == PartitionTag.STATIC) {
        fkId = PKGenerator.downForStaticPkId(this, refToTable, fkId);
      } else if (partitionTag == PartitionTag.DYNAMIC_EXISTS) {
        fkId = PKGenerator.downForStaticOrDynamicExistsPkId(this, refToTable, fkId);
      } else {
        throw new RuntimeException("这种情况不应该出现");
      }

      for (Attribute attr : fk) {
        Attribute refToAttr = fk.findReferencedAttrByFkAttr(attr);
        PartitionTag refToPartitionTag =
            PKGenerator.calcPartition(fkId, refToTableMirror.getPkPartitionAlg());

        // 对于动态区数据，且其引用的数据源自动态区，则需要构造引用信息
        if (partitionTag == PartitionTag.DYNAMIC_EXISTS
            && refToPartitionTag == PartitionTag.DYNAMIC_EXISTS) {
          Reference reference = new Reference(table, attr, pkId, refToTable, refToAttr, fkId);
          refInfo.refFromMap.put(RefInfo.fromKey(table, attr), reference);
          // 反向引用信息
          Record refToRecord = refToTableMirror.dynamicRecordMap.get(fkId);
          refToRecord.getRefInfo().refToMap.put(RefInfo.toKey(table, attr, pkId), reference);
        }

        // 去掉了前缀外键，不存在级联引用
        AttrValue attrValue =
            FKGenerator.fkId2Fk(
                fkId,
                refToAttr.getAttrType(),
                this.getTableMirrorById(refToTable.getTableId())
                    .getPkParamById(refToAttr.getAttrId()));
        col2val.put(attr.getAttrName(), attrValue);
      }
    }

    return col2val;
  }

  /**
   * 根据tableId获取对应的TableGenModel
   *
   * @param tableId tableId
   * @return tableGenModel
   */
  public TableMirror getTableMirrorById(int tableId) {
    return tableMirrorMap.get(tableId);
  }

  /**
   * 获取该miniShadow所使用的的ValueListRepo
   *
   * @return ValueListRepo
   */
  public DataRepo getDateRepo() {
    return this.dataRepo;
  }

  /**
   * 根据pkId计算所有主键属性值
   *
   * @param tableId tableId
   * @param pkId pkId
   * @return map of pk attribute
   */
  public Map<String, AttrValue> getPkValueByPkId(int tableId, int pkId) {
    TableMirror tableMirror = getTableMirrorById(tableId);
    Table table = this.schema.findTableById(tableId);

    Map<String, AttrValue> attrValueMap = new HashMap<>();
    for (Attribute attr : table.getPrimaryKey()) {
      AttrValue attrValue =
          PKGenerator.pkId2Pk(
              pkId, attr.getAttrType(), tableMirror.getPkParamById(attr.getAttrId()));
      attrValueMap.put(attr.getAttrName(), attrValue);
    }

    return attrValueMap;
  }

  /**
   * 根据fkId计算外键属性值
   *
   * @param tableId tableId
   * @param fkId fkId
   * @return map of pk attribute
   */
  public Map<String, AttrValue> getFkValueByFkId(int tableId, int fkId, String fkAttrName) {
    TableMirror tableMirror = getTableMirrorById(tableId);
    Table table = this.schema.findTableById(tableId);
    Attribute attr = table.getAttributeByName(fkAttrName);

    ForeignKey foreignKey = table.findCommFKByAttrName(fkAttrName);

    Table refToTable = foreignKey.getReferencedTable();
    TableMirror refToTableMirror = getTableMirrorById(refToTable.getTableId());
    Attribute retToAttr = foreignKey.findReferencedAttrByFkAttr(attr);
    PKParam refToPkParam = refToTableMirror.getPkParamById(retToAttr.getAttrId());

    tableMirror.getFkParamById(fkId);

    Map<String, AttrValue> attrValueMap = new HashMap<>();

    attrValueMap.put(fkAttrName, FKGenerator.fkId2Fk(fkId, attr.getAttrType(), refToPkParam));

    return attrValueMap;
  }

  /**
   * 根据fkId计算外键属性值
   *
   * @param tableId tableId
   * @param index index
   * @param attrName attrName
   * @return map of pk attribute
   */
  public Map<String, AttrValue> getAttrValueByIndex(int tableId, int index, String attrName) {
    Table table = this.schema.findTableById(tableId);
    Attribute attr = table.getAttributeByName(attrName);

    Map<String, AttrValue> attrValueMap = new HashMap<>();
    attrValueMap.put(attrName, AttrGenerator.index2attr(index, attr.getAttrType(), getDateRepo()));

    return attrValueMap;
  }

  /**
   * @param tableId tableId
   * @param attrName attrName
   * @param value real attrValue
   * @return Pair: attrName -> logicalValue
   */
  public Pair<String, Integer> getLogicalValue(int tableId, String attrName, String value) {

    TableMirror tableMirror = this.getTableMirrorById(tableId);
    Table table = this.schema.getTableList().get(tableId);

    if ("pkId".equals(attrName)) {
      return new Pair<>(attrName, Integer.parseInt(value));
    } else if (attrName.startsWith(Symbol.PK_ATTR_PREFIX)) {
      int pkAttrId = Integer.parseInt(attrName.substring(Symbol.PK_ATTR_PREFIX.length()));

      DataType attrType = table.findAttrTypeByAttrName(attrName);
      AttrValue attrValue = AttrValue.parse(attrType, value);
      logger.info("value: %s", value);
      PKParam pkParam = tableMirror.getPkParamById(pkAttrId);
      int pkId = PKGenerator.pk2PkId(attrValue, pkParam);
      return new Pair<>(attrName, pkId);
    } else if (attrName.startsWith(Symbol.FK_ATTR_PREFIX)) {
      String[] gid_id = attrName.substring(Symbol.FK_ATTR_PREFIX.length()).split("_");
      int fkAttrGroupId = Integer.parseInt(gid_id[0]);
      int fkAttrId = Integer.parseInt(gid_id[1]);

      ForeignKey foreignKey = table.getCommonForeignKeyList().get(fkAttrGroupId);
      Attribute fkAttr = foreignKey.get(fkAttrId);
      Attribute refToAttr = foreignKey.findReferencedAttrByFkAttr(fkAttr);
      Table refToTable = foreignKey.getReferencedTable();

      Pair<String, Integer> pair =
          this.getLogicalValue(refToTable.getTableId(), refToAttr.getAttrName(), value);
      return new Pair<>(attrName, pair.getValue());
    } else if (attrName.startsWith(Symbol.COMM_ATTR_PREFIX)) {
      DataType attrType = table.findAttrTypeByAttrName(attrName);
      AttrValue attrValue = AttrValue.parse(attrType, value);
      int logicalValue = AttrGenerator.attr2index(attrValue, this.dataRepo);
      return new Pair<>(attrName, logicalValue);
    } else {
      throw new RuntimeException("不支持该类型" + attrName);
    }
  }

  /**
   * @param tableId tableId
   * @param valueMap attrName -> attrValue
   * @return Map: attrName -> logicalValue
   */
  public Map<String, String> getLogicalValueMap(int tableId, Map<String, String> valueMap) {
    Map<String, String> resMap = new HashMap<>();
    for (String attrName : valueMap.keySet()) {
      Pair<String, Integer> pair = getLogicalValue(tableId, attrName, valueMap.get(attrName));
      resMap.put(pair.getKey(), pair.getValue().toString());
    }
    return resMap;
  }

  public Map<String, String> getLogicalValueMapForAttrValue(
      int tableId, Map<String, AttrValue> valueMap) {
    Map<String, String> resMap = new HashMap<>();
    for (String attrName : valueMap.keySet()) {
      Pair<String, Integer> pair =
          getLogicalValue(tableId, attrName, valueMap.get(attrName).value.toString());
      resMap.put(pair.getKey(), pair.getValue().toString());
    }
    return resMap;
  }

  public Map<String, String> getRealValueMapForAttrValue(Map<String, AttrValue> valueMap) {
    Map<String, String> resMap = new HashMap<>();
    for (String attrName : valueMap.keySet()) {
      resMap.put(attrName, valueMap.get(attrName).value.toString());
    }
    return resMap;
  }

  /**
   * 根据CSV文件中的一行记录，获取对应的逻辑值valueMap
   *
   * @param tableId tableId
   * @param tableHeader 表头
   * @param recordLine CSV文件中的一行记录
   * @return LogicalValueMap。其中，key:属性名称(不包含主键和PKID)，value：属性对应的逻辑值，含义等价于TupleTrace.valueMap
   */
  public Map<String, String> getValueMapFromCSV(
      int tableId, String tableHeader, String recordLine) {
    String[] keys = tableHeader.trim().split(",");
    String[] values = recordLine.trim().split(",");

    // 数据清洗
    keys = Arrays.stream(keys).map(key -> StringUtils.strip(key, "\"'")).toArray(String[]::new);
    values =
        Arrays.stream(values)
            .map(value -> value.substring(1, value.length() - 1))
            .toArray(String[]::new);

    Map<String, String> valueMap = new HashMap<>();

    for (int ind = 0; ind < keys.length; ind++) {
      if (keys[ind].startsWith("pkId") || keys[ind].startsWith(Symbol.PK_ATTR_PREFIX)) {
        continue;
      }

      valueMap.put(keys[ind], values[ind]);
    }

    return getLogicalValueMap(tableId, valueMap);
  }

  /**
   * 根据 ParamInfo 和 logicalValue 获取真实值
   *
   * @param paramInfo paramInfo
   * @param logicalValue logicalValue
   * @return attrValue
   */
  public AttrValue getAttrValueByParamInfo(ParamInfo paramInfo, int logicalValue) {
    switch (paramInfo.type) {
      case PK:
        return PKGenerator.pkId2Pk(
            logicalValue,
            paramInfo.attr.getAttrType(),
            this.getTableMirrorById(paramInfo.table.getTableId())
                .getPkParamById(paramInfo.attr.getAttrId()));
      case FK:
        return PKGenerator.pkId2Pk(
            logicalValue,
            paramInfo.attr.getAttrType(),
            this.getTableMirrorById(paramInfo.fk.getReferencedTable().getTableId())
                .getPkParamById(
                    paramInfo.fk.findReferencedAttrByFkAttr(paramInfo.attr).getAttrId()));
      case ATTR:
        return AttrGenerator.index2attr(logicalValue, paramInfo.attr.getAttrType(), this.dataRepo);
      case UK:
        return UKGenerator.index2attr(logicalValue, paramInfo.attr.getAttrType(), this.dataRepo);
      default:
        throw new RuntimeException("暂不支持该类型：" + paramInfo.type.name());
    }
  }

  /**
   * 计算 tableId.pkId 的初始分区
   *
   * @param tableId tableId
   * @param pkId pkId
   * @return partitionTag
   */
  public PartitionTag getPartitionTag(int tableId, int pkId) {
    return this.tableMirrorMap.get(tableId).getPkPartitionAlg().partition(pkId);
  }

  /** 备份DBMirror状态 */
  public void backup() {
    for (Integer key : tableMirrorMap.keySet()) {
      TableMirror tableMirror = tableMirrorMap.get(key);
      tableMirrorMapBackup.put(key, tableMirror.clone());
    }
  }

  /** 恢复DBMirror至backup状态 */
  public void restore() {
    tableMirrorMap.clear();
    for (Integer key : tableMirrorMapBackup.keySet()) {
      TableMirror tableMirror = tableMirrorMapBackup.get(key);
      tableMirrorMap.put(key, tableMirror.clone());
    }
  }

  /** 根据名字获取table，如果有重名的（理论上没有），会返回第一个 */
  public AbstractTable getTableByName(String tableName) {
    for (Table table : schema.getTableList()) {
      if (table.getTableName().equals(tableName)) {
        return table;
      }
    }

    for (View view : schema.getViewList()) {
      if (view.getTableName().equals(tableName)) {
        return view;
      }
    }
    return null;
  }
}
