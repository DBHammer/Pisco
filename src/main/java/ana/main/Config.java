package ana.main;

import ana.verify.adapter.*;
import config.analysis.AnalysisConfig;
import context.OrcaContext;
import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** 封装整个程序用到的常量 */
public class Config {

  private static final Logger logger = LogManager.getLogger(Config.class);

  private static final AnalysisConfig analysisConfig = null;

  public static long PERIOD_FOOTPRINT_STATISTIC;

  public static int NUMBER_THREAD;

  public static int NUMBER_ROTATION;

  public static int INITIAL_ROTATION;

  public static int TERMINAL_ROTATION;

  public static int PRIVATE_BUFFER_SIZE;

  public static int VERSION_CHAIN_PURGE_LENGTH;

  public static int GRAPH_PURGE_SIZE;

  public static boolean TRACK_WW;

  public static boolean TRACK_WR;

  public static boolean TRACK_RW;

  public static boolean LAUNCH_GC;

  public static boolean TAKE_VC;

  public static boolean CALCULATE_INITIAL_DATA;

  public static boolean STATISTIC_FOOTPRINT;

  public static boolean OUTPUT_RUNTIME;

  public static boolean VERIFY;

  public static boolean PG_OPTIMIZATION;

  public static boolean CLOSE_CYCLE_DETECTION;

  public static String ANALYSIS_TARGET_DIR;

  public static String JSON_TRACE_OUTPUT_DIRECTORY;

  public static String OBJECT_COLLECTION;

  public static String CERTIFICATE_OUTPUT_FILE;

  public static String DEBUG_OUTPUT_FILE;

  public static String UNIFIED_OUTPUT_FILE;

  public static String PERFORMANCE_OUTPUT_FILE;

  public static String VISUALIZE_OUTPUT_FILE;

  public static String SNAPSHOT_POINT;

  public static Adapter adapter;

  public static boolean PERFORMANCE_STATISTIC;

  public static void initialize(
      String dbType,
      String analysisTargetDir,
      String outputDir,
      String snapshotPoint,
      boolean performanceStatistic) {
    switch (dbType) {
      case "mysql":
        adapter = new MySQLAdapter();
        break;
      case "oceanbase":
        adapter = new OceanbaseAdapter();
        break;
      case "tidb":
        adapter = new TiDBAdapter();
        break;
      case "postgres":
        adapter = new PostgreSQLAdapter();
        break;
      case "gauss":
        adapter = new GaussAdapter();
        break;
      case "sqlite":
        adapter = new SQLiteAdapter();
        break;
      case "crdb":
        adapter = new CRDBAdapter();
        break;
      case "ydb":
        adapter = new YDBAdapter();
        break;
      case "oracle":
        adapter = new OracleAdapter();
        break;
      case "tdsql":
        adapter = new TDSQLAdapter();
        break;
    }

    if (adapter == null) {
      throw new RuntimeException("Not Supported DBMS");
    }

    try {
      // loaderConfig = LoaderConfig.parse(loaderConfigPath);
      // analysisConfig = AnalysisConfig.parse(analysisConfigPath);
      NUMBER_ROTATION = 1;
      INITIAL_ROTATION = 0;
      TERMINAL_ROTATION = 1;
      PERIOD_FOOTPRINT_STATISTIC = 1000;
      PRIVATE_BUFFER_SIZE = 10;
      VERSION_CHAIN_PURGE_LENGTH = 100;
      GRAPH_PURGE_SIZE = 100;
      TRACK_WW = false;
      TRACK_WR = false;
      TRACK_RW = false;
      LAUNCH_GC = true;
      TAKE_VC = true;
      CALCULATE_INITIAL_DATA = false;
      STATISTIC_FOOTPRINT = false;
      OUTPUT_RUNTIME = true;
      VERIFY = true;
      CLOSE_CYCLE_DETECTION = true;
      PG_OPTIMIZATION = false;

      ANALYSIS_TARGET_DIR = analysisTargetDir;
      JSON_TRACE_OUTPUT_DIRECTORY = analysisTargetDir + "/trace/";
      OBJECT_COLLECTION = analysisTargetDir + "/ObjectCollection.obj";

      CERTIFICATE_OUTPUT_FILE = outputDir + "/certificate";
      DEBUG_OUTPUT_FILE = outputDir + "/debug.json";
      UNIFIED_OUTPUT_FILE = outputDir + "/unified.json";
      PERFORMANCE_OUTPUT_FILE = outputDir + "/performance.json";
      VISUALIZE_OUTPUT_FILE = outputDir + "/visOutput.html";

      SNAPSHOT_POINT = snapshotPoint;

      int threadID = 0;
      File[] list = new File(JSON_TRACE_OUTPUT_DIRECTORY).listFiles();
      Set<String> set = new HashSet<>();
      assert list != null;
      for (File file : list) {
        String fileName = file.getName();
        set.add(fileName.substring(0, fileName.indexOf(".")));
      }
      if (OrcaContext.configColl != null) {
        NUMBER_THREAD = OrcaContext.configColl.getLoader().getNumberOfLoader();
      } else {
        NUMBER_THREAD = set.size();
      }

    } catch (Exception e) {
      logger.warn(e);
    }

    PERFORMANCE_STATISTIC = performanceStatistic;
  }
}
