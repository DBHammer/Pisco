package gen.data.type;

import org.apache.commons.lang3.NotImplementedException;

public enum DataType {
  INTEGER,
  VARCHAR,
  DOUBLE,
  DECIMAL,
  TIMESTAMP,
  BLOB,
  BOOL;

  public static DataType dataTypeConst2enum(int typeConst) {
    switch (typeConst) {
      case DataTypeConst.INTEGER:
        return INTEGER;
      case DataTypeConst.VARCHAR:
        return VARCHAR;
      case DataTypeConst.DOUBLE:
        return DOUBLE;
      case DataTypeConst.DECIMAL:
        return DECIMAL;
      case DataTypeConst.BLOB:
        return BLOB;
      case DataTypeConst.TIMESTAMP:
        return TIMESTAMP;
      case DataTypeConst.BOOL:
        return BOOL;
      default:
        throw new NotImplementedException();
    }
  }
}
