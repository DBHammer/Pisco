package ana.io;

import ana.main.Config;
import java.io.IOException;
import java.util.ArrayList;
import trace.BufferedTransactionTraceReader;
import trace.OperationTrace;

/**
 * Trace文件io相关操作
 *
 * @author like_
 */
public class HandlerFactory {

  /** 处理每一个trace file的io句柄 */
  private static ArrayList<BufferedTransactionTraceReader> fileHandler;

  /** 初始化fileHandler和rotation */
  public static void initialize() {
    fileHandler = new ArrayList<>();
    for (int threadID = 0; threadID < Config.NUMBER_THREAD; threadID++) {
      fileHandler.add(null);
    }
  }

  /**
   * 从文件threadID中取一行OperationTrace返回,如果文件中没有trace了，返回null
   *
   * @param threadID 用于组成文件名的线程id
   * @return trace/null(已全部取出时)
   */
  public static OperationTrace nextOperationTrace(int threadID) {
    try {
      if (fileHandler.get(threadID) == null) {
        BufferedTransactionTraceReader traceReader =
            new BufferedTransactionTraceReader(
                Config.JSON_TRACE_OUTPUT_DIRECTORY + "loader-" + threadID);
        //				System.err.println(Config.JSON_TRACE_OUTPUT_DIRECTORY +
        //						"loader-" + threadID);
        traceReader.begin();
        fileHandler.set(threadID, traceReader);
      }
      if (fileHandler.get(threadID).hasNext()) {
        return fileHandler.get(threadID).readOperationTrace();
      } else {
        return null;
      }
    } catch (IOException e) {
      throw new RuntimeException("Read Operation Trace Error: " + e.getMessage());
    }
  }
}
