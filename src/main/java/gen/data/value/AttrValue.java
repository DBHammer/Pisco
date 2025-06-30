package gen.data.value;

import gen.data.type.DataType;
import java.sql.Timestamp;
import org.apache.commons.lang3.NotImplementedException;

public class AttrValue {
  public final DataType type;
  public final Object value;

  public AttrValue(DataType type, Object value) {
    super();
    this.type = type;
    this.value = value;
  }

  public static AttrValue parse(DataType type, String value) {
    switch (type) {
      case INTEGER:
        return new AttrValue(DataType.INTEGER, Integer.parseInt(value));
      case VARCHAR:
        return new AttrValue(DataType.VARCHAR, value);
      case DOUBLE:
        return new AttrValue(DataType.DOUBLE, Double.parseDouble(value));
      case DECIMAL:
        return new AttrValue(DataType.DECIMAL, Double.parseDouble(value));
      case TIMESTAMP:
        return new AttrValue(DataType.TIMESTAMP, Timestamp.valueOf(value));
      case BOOL:
        return new AttrValue(DataType.BOOL, Boolean.parseBoolean(value));
      default:
        throw new RuntimeException("暂不支持该类型 " + type.name());
    }
  }

  public boolean lessThen(AttrValue v2) {
    switch (type) {
      case INTEGER:
        return (int) this.value < (int) v2.value;
      case VARCHAR:
        return ((String) this.value).compareTo((String) v2.value) < 0;
      case DOUBLE:
      case DECIMAL:
        return (double) this.value < (double) v2.value;
      case TIMESTAMP:
        return ((Timestamp) this.value).getTime() < ((Timestamp) v2.value).getTime();
      case BLOB:
      case BOOL:
      default:
        throw new NotImplementedException();
    }
  }
}
