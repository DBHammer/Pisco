package ana.output;

import ana.main.Config;
import ana.main.OrcaVerify;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceStatisticThread implements Runnable {

  private static final Logger logger = LogManager.getLogger(PerformanceStatisticThread.class);

  private final Gson gson;

  private JsonWriter performanceIOHandler = null;

  public PerformanceStatisticThread() {
    gson = new Gson();

    try {
      performanceIOHandler =
          new JsonWriter(new BufferedWriter(new FileWriter(Config.PERFORMANCE_OUTPUT_FILE)));
      performanceIOHandler.setIndent("    ");
      performanceIOHandler.beginArray();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  @Override
  public void run() {
    int counter = 0;
    FootprintStatistic sumFootprintStatistic = new FootprintStatistic();
    FootprintStatistic maxFootprintStatistic = new FootprintStatistic();

    while (true) {
      // 1定期统计空间和时间指标
      counter++;
      long startTS = System.currentTimeMillis();
      if (Config.STATISTIC_FOOTPRINT) {
        gson.toJson(
            FootprintStatistic.outputFootprintStatistic(
                sumFootprintStatistic, maxFootprintStatistic),
            FootprintStatisticHuman.class,
            performanceIOHandler);
      }
      if (Config.OUTPUT_RUNTIME) {
        gson.toJson(
            new RuntimeStatisticHuman(OrcaVerify.runtimeStatistic),
            RuntimeStatisticHuman.class,
            performanceIOHandler);
      }

      try {
        performanceIOHandler.flush();
      } catch (IOException e) {
        logger.warn(e);
      }
      long finishTS = System.currentTimeMillis();

      if (!OrcaVerify.mainThread.isAlive()) {
        // 2终止统计，如果主线程任然活跃，那么继续统计性能信息
        try {
          sumFootprintStatistic.average(counter);

          OrcaVerify.runtimeStatistic.setUserRealTime();

          // 2.3.1输出空间占用的平均值
          gson.toJson(
              new FootprintStatisticHuman(sumFootprintStatistic),
              FootprintStatisticHuman.class,
              performanceIOHandler);

          // 2.3.2输出空间占用的最大值
          gson.toJson(
              new FootprintStatisticHuman(maxFootprintStatistic),
              FootprintStatisticHuman.class,
              performanceIOHandler);

          // 2.3.3输出时间性能统计信息
          gson.toJson(
              new RuntimeStatisticHuman(OrcaVerify.runtimeStatistic),
              RuntimeStatisticHuman.class,
              performanceIOHandler);

          // 2.3.4负载特征数量统计信息
          gson.toJson(OrcaVerify.numberStatistic, NumberStatistic.class, performanceIOHandler);

          performanceIOHandler.endArray();
          performanceIOHandler.flush();

        } catch (IOException e) {
          logger.warn(e);
        }

        // 2.3.3刷新屏幕的缓冲，该线程是整个程序最后一个结束的Thread
        System.out.flush();
        try {
          performanceIOHandler.close();
        } catch (IOException e) {
          logger.warn(e);
        }
        break;
      } else {
        // 3.周期，使得Config.PERIOD_FOOTPRINT_STATISTIC有且会有一次性能统计
        if (Config.PERIOD_FOOTPRINT_STATISTIC > (finishTS - startTS)) {
          try {
            Thread.sleep(Config.PERIOD_FOOTPRINT_STATISTIC - finishTS + startTS);
          } catch (InterruptedException e) {
            logger.warn(e);
          }
        } else {
          if (Config.PERIOD_FOOTPRINT_STATISTIC - finishTS + startTS < -1000) {
            throw new RuntimeException("should increase PERIOD FOOTPRINT STATISTIC");
          }
        }
      }
    }
  }
}
