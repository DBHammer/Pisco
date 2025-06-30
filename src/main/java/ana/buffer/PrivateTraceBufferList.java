package ana.buffer;

import ana.main.Config;
import java.util.ArrayList;

/** 管理所有private buffer的数据结构 */
public class PrivateTraceBufferList {
  /** 核心数据结构：所有trace file的缓存 */
  private static ArrayList<PrivateTraceBuffer> privateBufferList;

  /** 初始化每一个buffer */
  public static void initialize() {
    privateBufferList = new ArrayList<>();

    for (int bufferID = 0; bufferID < Config.NUMBER_THREAD; bufferID++) {
      privateBufferList.add(new PrivateTraceBuffer());
    }
  }

  public static PrivateTraceBuffer getPrivateBuffer(int bufferID) {
    return privateBufferList.get(bufferID);
  }

  /**
   * 返回该类中的所有对象，服务于内存开销的统计
   *
   * @return all objects in the private buffer
   */
  public static Object[] getAllObject() {
    return new Object[] {privateBufferList};
  }
}
