package util.xml;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

public class DocumentUtils {
  private static final Logger logger = LogManager.getLogger(DocumentUtils.class);
  /**
   * dom4j文档转字符串
   *
   * @param document dom4j文档对象
   * @return 格式化的xml字符串
   */
  public static String doc2str(Document document) {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    // 设置生成xml的格式
    OutputFormat format = OutputFormat.createPrettyPrint();
    // 设置编码格式
    format.setEncoding("UTF-8");

    try {
      XMLWriter writer = new XMLWriter(outStream, format);
      writer.write(document);
      writer.close();
    } catch (IOException e) {
      logger.warn(e);
    }
    return outStream.toString();
  }
}
