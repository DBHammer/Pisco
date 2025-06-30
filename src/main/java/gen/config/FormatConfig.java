package gen.config;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

public class FormatConfig {

  public FormatConfig() {
    super();
  }

  public Document genFormatConfig() {
    Document document = DocumentHelper.createDocument();
    Element format = document.addElement("format");

    length_limit_with_parent(format, "integer", "11");
    length_limit_with_parent(format, "varchar", "255");
    double_decimal(format);
    length_limit_with_parent(format, "blob", "255");

    return document;
  }

  private void length_limit_with_parent(
      Element attachElement, String parentName, String length_limit) {
    attachElement.addElement(parentName).addElement("length_limit").setText(length_limit);
  }

  private void double_decimal(Element format) {
    Element double_decimal = format.addElement("double_decimal");
    double_decimal.addElement("decimal_precision_limit").setText("10");
    double_decimal.addElement("decimal_scale_limit").setText("0");
    double_decimal.addElement("double_precision_limit").setText("38");
    double_decimal.addElement("double_scale_limit").setText("17");
  }
}
