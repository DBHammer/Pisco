package util.log;

import org.junit.Test;

public class LogUtilsTest {

  @Test
  public void setRootLoggerLevel() {
    LogUtils.printAllLoggerLevel();
    LogUtils.setAllLoggerLevel("warn");
    LogUtils.printAllLoggerLevel();
  }
}
