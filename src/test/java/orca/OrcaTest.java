package orca;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Locale;
import main.Orca;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.DocumentException;
import org.junit.Test;
import util.log.LogUtils;

public class OrcaTest {
  private static final Logger logger = LogManager.getLogger(OrcaTest.class);

  @Test
  public void mainTest()
      throws DocumentException, SQLException, IOException, InterruptedException,
          ClassNotFoundException {

    LogUtils.setAllLoggerLevel("warn");
    LogUtils.setLoggerLevel("orca.OrcaTest", "info");

    String[] dbTypes = new String[] {"mysql", "postgresql"};
    String dbName = "db0";
    String verticalSeparator = new String(new char[50]).replace("\0", "-");

    for (String dbType : dbTypes) {
      logger.info(verticalSeparator);
      logger.info(String.format("Test for %s", dbType.toUpperCase(Locale.ROOT)));
      logger.info(verticalSeparator);
      String[] args = new String[] {dbType, dbName};
      Orca.main(args);
    }
  }

  @Test
  public void commonTest() {}
}
