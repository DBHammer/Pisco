package adapter;

import gen.data.format.DataFormat;
import gen.data.type.DataType;
import gen.data.value.AttrValue;
import java.util.HashMap;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.Test;

public class AdapterMySQLTest {

  @Test
  public void insert() throws DocumentException {
    String formatConfigPath = "src/main/resources/model/format.xml";
    Adapter adapter = new AdapterMySQL(new DataFormat(new SAXReader().read(formatConfigPath)));
    HashMap<String, AttrValue> col2val = new HashMap<>();
    col2val.put("intcol", new AttrValue(DataType.INTEGER, 123));
    col2val.put("strcol", new AttrValue(DataType.VARCHAR, "stringss"));
    col2val.put("boolcol", new AttrValue(DataType.BOOL, true));
    String sql = adapter.insert("table999", col2val);
    System.out.println(sql);
  }
}
