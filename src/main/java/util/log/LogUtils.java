package util.log;

import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

public class LogUtils {
  /**
   * 设置所有 Logger 的 level
   *
   * @param newLevel 目标level
   */
  public static void setAllLoggerLevel(String newLevel) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

    org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
    Map<String, LoggerConfig> loggerConfigs = config.getLoggers();
    loggerConfigs.forEach((name, loggerConfig) -> loggerConfig.setLevel(Level.valueOf(newLevel)));
    ctx.updateLoggers(config);
  }

  /**
   * 设置某一个 Logger 的 level
   *
   * @param loggerName Logger 名字
   * @param newLevel 目标 level
   */
  public static void setLoggerLevel(String loggerName, String newLevel) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

    org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
    Map<String, LoggerConfig> loggerConfigs = config.getLoggers();
    loggerConfigs.forEach(
        (name, loggerConfig) -> {
          if (loggerName.equals(name)) {
            loggerConfig.setLevel(Level.valueOf(newLevel));
          }
        });
    ctx.updateLoggers(config);
  }

  /** 打印所有 Logger 的 level */
  public static void printAllLoggerLevel() {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);

    org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
    Map<String, LoggerConfig> loggerConfigs = config.getLoggers();
    loggerConfigs.forEach(
        (name, loggerConfig) -> System.out.printf("%s %s\n", name, loggerConfig.getLevel()));
    ctx.updateLoggers(config);
  }
}
