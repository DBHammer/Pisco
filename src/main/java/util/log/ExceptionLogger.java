package util.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.org.apache.commons.lang3.exception.ExceptionUtils;

public class ExceptionLogger {
  private static final Logger logger = LogManager.getLogger(ExceptionLogger.class);

  public static void error(String msg) {
    logger.error(msg);
  }

  public static void error(Exception e) {
    logger.error(ExceptionUtils.getStackTrace(e));
  }
}
