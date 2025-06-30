package gen.data.format;

import java.io.Serializable;
import org.dom4j.Document;
import org.dom4j.Element;

/** 根据xml，确定每种数据类型的格式，一旦确定，整个测试过程不变 */
public class DataFormat implements Serializable {

  public int integerLength;
  public int varcharLength;
  public int decimalPrecision;
  public int decimalScale;
  public int doublePrecision;
  public int doubleScale;
  public int blobLength;

  public DataFormat(Document operationConfig) {
    super();
    Element root = operationConfig.getRootElement();
    this.integerLength = Integer.parseInt(root.selectSingleNode("integer/length_limit").getText());
    this.varcharLength = Integer.parseInt(root.selectSingleNode("varchar/length_limit").getText());
    this.decimalPrecision =
        Integer.parseInt(root.selectSingleNode("double_decimal/decimal_precision_limit").getText());
    this.decimalScale =
        Integer.parseInt(root.selectSingleNode("double_decimal/decimal_scale_limit").getText());
    this.doublePrecision =
        Integer.parseInt(root.selectSingleNode("double_decimal/double_precision_limit").getText());
    this.doubleScale =
        Integer.parseInt(root.selectSingleNode("double_decimal/double_scale_limit").getText());
    this.blobLength = Integer.parseInt(root.selectSingleNode("blob/length_limit").getText());
  }

  public DataFormat() {}

  /** 同意格式 */
  public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
}
