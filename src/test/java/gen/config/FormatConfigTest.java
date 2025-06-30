package gen.config;

import org.junit.Test;
import util.xml.DocumentUtils;

public class FormatConfigTest {

  @Test
  public void genFormatConfig() {
    System.out.println(DocumentUtils.doc2str(new FormatConfig().genFormatConfig()));
  }
}
