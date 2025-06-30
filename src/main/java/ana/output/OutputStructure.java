package ana.output;

import ana.main.Config;
import com.google.gson.Gson;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutputStructure {

  private static final Logger logger = LogManager.getLogger(OutputCertificate.class);

  /** 将对象按json格式输出 */
  private static Gson gson = null;

  /** 写io handler */
  private static BufferedWriter debugIOHandler = null;

  /** 将所有trace整合到统一的文件中 */
  private static BufferedWriter unifiedIOHandler = null;

  private static void initialize() {
    gson = new Gson();

    try {
      debugIOHandler =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(Config.DEBUG_OUTPUT_FILE)));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }

    try {
      unifiedIOHandler =
          new BufferedWriter(
              new OutputStreamWriter(new FileOutputStream(Config.UNIFIED_OUTPUT_FILE)));
    } catch (FileNotFoundException e) {
      logger.warn(e);
    }
  }

  /** 将想输出的结构输出，方便随意改写，外部代码随时都可以调用 */
  public static void outputStructure(Object object) {
    try {
      if (debugIOHandler == null) {
        initialize();
      }

      debugIOHandler.write(gson.toJson(object));
      debugIOHandler.newLine();
      debugIOHandler.flush();
    } catch (IOException e) {
      logger.warn(e);
    }
  }

  /** 将想输出的结构输出，方便随意改写，外部代码随时都可以调用 */
  public static void outputTrace(Object object) {
    try {
      if (unifiedIOHandler == null) {
        initialize();
      }

      unifiedIOHandler.write(gson.toJson(object));
      unifiedIOHandler.newLine();
      unifiedIOHandler.flush();
    } catch (IOException e) {
      logger.warn(e);
    }
  }
}
