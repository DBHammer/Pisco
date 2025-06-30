package adapter;

import gen.data.format.DataFormat;

public class AdapterHelper {
  /**
   * 根据 dbType 获取 Adapter 对象
   *
   * @param dbType database type
   * @param dataFormat data format object
   * @return adapter for dbType
   */
  public static Adapter resolveAdapter(String dbType, DataFormat dataFormat) {
    Adapter adapter;
    switch (dbType) {
      case "mysql":
        adapter = new AdapterMySQL(dataFormat);
        break;
      case "postgresql":
        adapter = new AdapterPostgreSQL(dataFormat);
        break;
      case "oceanbase":
        adapter = new AdapterOceanBase(dataFormat);
        break;
      case "tidb":
        adapter = new AdapterTiDB(dataFormat);
        break;
      case "gauss":
        adapter = new AdapterGauss(dataFormat);
        break;
      case "sqlite":
        adapter = new AdapterSQLite(dataFormat);
        break;
      case "yugabyte":
        adapter = new AdapterYugabyte(dataFormat);
        break;
      case "tdsql":
        adapter = new AdapterTDSQL(dataFormat);
        break;
      default:
        throw new RuntimeException("不支持的数据库类型");
    }

    return adapter;
  }
}
