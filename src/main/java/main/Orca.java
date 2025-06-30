package main;

import static context.OrcaContext.configColl;
import static context.OrcaContext.ioPath;

import adapter.Adapter;
import adapter.AdapterHelper;
import config.ConfigCollection;
import config.jdbc.DataSourceConfig;
import dbloader.scheduler.OrcaScheduler;
import dbloader.util.QueryExecutor;
import dbloader.util.SchemaLoader;
import gen.data.InitialData;
import gen.operation.TransactionCaseRepo;
import gen.schema.Schema;
import gen.shadow.DBMirror;
import io.IOPath;
import io.IOUtils;
import io.ObjectCollection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.jdbc.DataSourceUtils;

public class Orca {
  private static final Logger logger = LogManager.getLogger(Orca.class);

  /** @param args dbType, dbName, configFile, logLevel */
  public static void main(String[] args) throws IOException, SQLException, InterruptedException {

    if (args.length != 3) {
      Main.printUsageAndExit();
    }

    String configDir = args[1];
    String outputDir = args[2];

    // 初始化配置与输出路径
    ioPath = new IOPath(configDir, outputDir);
    // 解析配置
    configColl = ConfigCollection.parse(configDir);

    String dbType = configColl.getDatasource().getPlatform();
    String url = configColl.getDatasource().getUrl();
    String dbName = url.substring(url.lastIndexOf("/") + 1, url.indexOf("?"));

    load(dbType, dbName, outputDir);
  }

  private static void load(String dbType, String dbName, String outputDir)
      throws SQLException, IOException, InterruptedException {

    // 清理输出文件夹
    ioPath.cleanForLoad();

    // 配置数据库信息（需要在DataSourceUtils使用之前配置）
    // 需要与c3p0配置文件的信息保持一致
    initDataSource(dbType, dbName);

    // 生成schema
    logger.info("Generating Schema......");
    Schema schema = new Schema(configColl.getSchema());
    logger.info("Writing Schema as SQL......");
    schema.writeSQL(ioPath.schemaSqlDest);

    // 初始化MiniShadow
    // 初始化MiniShadow的过程中
    // 将会根据数据生成模型确定各个主键、外键、普通属性组的生成参数
    logger.info("Generating MiniShadow......");
    DBMirror dbMirror = new DBMirror(schema, configColl.getDataGenerate());

    // 产生一批事务模板供加载线程使用
    logger.info("Generating TransactionCaseRepo......");
    TransactionCaseRepo transactionCaseRepo =
        new TransactionCaseRepo(
            configColl.getLoader().getNumberOfTransactionCase(),
            configColl.getTransaction(),
            schema,
            DataSourceUtils.getAdapter().getSQLFilter());

    logger.info("Writing TransactionCaseRepo as SQL......");
    transactionCaseRepo.writeSQL(outputDir);

    // 存储 schema, transactionCaseRepo, miniShadow 等关键对象
    // 从而便于复现程序执行过程
    ObjectCollection objectCollection =
        new ObjectCollection(schema, transactionCaseRepo, dbMirror.getMirrorData());
    logger.info("Writing ObjectCollection as Object...");
    objectCollection.store(ioPath.objectCollectionDest);

    // 加载到数据库
    loadToDatabase(dbType, dbName, schema.toSQLList(), dbMirror, transactionCaseRepo);
  }

  private static void loadToDatabase(
      String dbType,
      String dbName,
      List<String> schema,
      DBMirror dbMirror,
      TransactionCaseRepo transactionCaseRepo)
      throws SQLException, IOException, InterruptedException {
    loadData2DB(dbType, dbName, schema, dbMirror);

    OrcaScheduler scheduler =
        new OrcaScheduler(
            configColl.getLoader().getIter(),
            configColl,
            dbMirror,
            ioPath,
            transactionCaseRepo,
            DataSourceUtils.getAdapter());

    long startNanoTime = System.nanoTime();
    scheduler.schedule();
    long finishNanoTime = System.nanoTime();
    IOUtils.writeString(
        String.valueOf(finishNanoTime - startNanoTime),
        ioPath.outputDir + "/scheduler.time",
        false);

    logger.info("************Good Bye*************");
    QueryExecutor.executor.shutdownNow();
  }

  public static void loadData2DB(
      String dbType, String dbName, List<String> schema, DBMirror dbMirror)
      throws SQLException, InterruptedException, IOException {
    // 将schema导入数据库中
    logger.info(String.format("Loading schema to database: %s-%s", dbType, dbName));
    SchemaLoader.loadToDB(dbName, schema);

    // 初始数据生成
    logger.info("Generating initial data......");
    InitialData initialData = dbMirror.initData(DataSourceUtils.getAdapter());
    // 存储数据 CSV
    logger.info("Writing initial data as CSV......");
    initialData.writeCSV(ioPath.initialDataDir);

    if (configColl.getLoader().isUseLoadInfile()) {

      // 数据导入数据库
      logger.info(String.format("Loading initial data to database: %s-%s", dbType, dbName));
      DataSourceUtils.loadCSV(initialData, ioPath.initialDataDir);
    } else {
      // 存储数据SQL
      logger.info("Writing initial data as SQL......");
      initialData.writeSQL(ioPath.initialSQLDest);
      // 数据导入数据库
      logger.info(String.format("Loading initial data to database: %s-%s", dbType, dbName));
      DataSourceUtils.loadInertList(initialData.toSQLList());
    }
  }

  public static void initDataSource(String dbType, String dbName) {
    // 配置数据库信息（需要在DataSourceUtils使用之前配置）
    // 需要与c3p0配置文件的信息保持一致
    DataSourceConfig dataSourceConfig = configColl.getDatasource();
    Adapter adapter = AdapterHelper.resolveAdapter(dbType, configColl.getDataFormat());
    DataSourceUtils.init(adapter, dataSourceConfig);
  }
}
