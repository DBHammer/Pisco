package ana.main;

import ana.buffer.ShareTraceBuffer;
import ana.buffer.ShareTraceBufferHeap;
import ana.io.HandlerFactory;
import ana.output.*;
import ana.thread.AnalysisThread;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import replay.controller.ReplayController;

public class OrcaVerify {
  public static void main(String[] args) {
    runtimeStatistic.initialUserRealTime();

    // 0.初始化配置信息
    if (args.length != 5) {
      printUsageAndExit();
    }

    // 数据库的类型
    String dbType = args[1];

    // 分析的对象，即生成程序的输出
    String analysisTargetDir = args[2];

    // 分析结果输出文件
    String outputDir = args[3];

    String snapshotPoint = args[4];

    File ouputfolder = new File(outputDir);
    if (!ouputfolder.exists() && !ouputfolder.isDirectory()) {
      ouputfolder.mkdirs();
    }

    // 1.初始化相关数据结构
    // config
    Config.initialize(dbType, analysisTargetDir, outputDir, snapshotPoint, false);

    // buffer
    HandlerFactory.initialize();
    ReplayController.init();
    shareTraceBuffer = new ShareTraceBufferHeap();

    // output
    ErrorStatistics.initialize();
    OutputCertificate.initialize();

    // analysis
    AnalysisThread.initialize(Config.adapter);

    OrcaVerify.logger.info("Start of Verification");
    OrcaVerify.logger.info("Target Database: " + dbType);
    OrcaVerify.logger.info("Target Workload: " + analysisTargetDir);
    OrcaVerify.logger.info("Output: " + outputDir);

    // 2.fork一个异步的线程，统计性能，并输出统计结果
    if (Config.PERFORMANCE_STATISTIC) {
      new Thread(new PerformanceStatisticThread()).start();
    }

    // 3.启动分析线程
    long startTS = System.nanoTime();
    analysisThread = new AnalysisThread();
    analysisThread.run();
    long finishTS = System.nanoTime();
    runtimeStatistic.setAnalysisTotalTime(finishTS - startTS);

    // 4.输出分析结果
    ErrorStatistics.outputStatistics();
    OutputCertificate.flush();
  }

  public static void printUsageAndExit() {
    System.err.println(
        "Usage: java -jar $jarfile database_type input_directory output_directory rr_snapshot_timepoint");
    System.err.println("Description:");
    System.err.println("       database_type: mysql/postgres/gauss");
    System.err.println("       input_directory: the output directory of the generator");
    System.err.println("       output_directory: the output directory of the verifier");
    System.err.println("       rr_snapshot_timepoint: start/nontrxctl");
    System.exit(-1);
  }

  // 主任务
  public static ShareTraceBuffer shareTraceBuffer;
  public static AnalysisThread analysisThread;

  // 用于性能统计
  public static final NumberStatistic numberStatistic = new NumberStatistic();
  public static final RuntimeStatistic runtimeStatistic = new RuntimeStatistic();
  public static final Thread mainThread = Thread.currentThread();

  // 用于输出命令行输出，全局唯一一个
  public static final Logger logger = LogManager.getLogger(OrcaVerify.class);
}
