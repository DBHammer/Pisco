package dbloader.util;

import adapter.Adapter;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import util.jdbc.DataSourceUtils;

public class SchemaLoader {
  private static String RESTORE_SUBFIX = "_restore";

  public static void loadToDB(String databaseName, List<String> schema) throws SQLException {
    Adapter adapter = DataSourceUtils.getAdapter();

    // recreate database
    adapter.dropDatabase(databaseName);
    adapter.createDatabase(databaseName);

    DataSourceUtils.loadInertList(schema);
  }

  public static void restoreDB(String dbName, Set<String> tableList) throws SQLException {
    Adapter adapter = DataSourceUtils.getAdapter();

    String restoreDBName = dbName + RESTORE_SUBFIX;
    for (String tableName : tableList) {
      // copy data from restoreDBName.tableName to dbName.tableName
      adapter.copyTable(restoreDBName, tableName, dbName, tableName);
    }
  }

  public static void copyDB(String dbName, Set<String> tableList, List<String> schema)
      throws SQLException {
    Adapter adapter = DataSourceUtils.getAdapter();

    String restoreDBName = dbName + RESTORE_SUBFIX;
    adapter.dropDatabase(restoreDBName);
    adapter.createDatabase(restoreDBName);

    for (String tableName : tableList) {
      adapter.copyTableSchema(dbName, tableName, restoreDBName, tableName);
      // copy data from dbName.tableName to restoreDBName.tableName
      adapter.copyTable(dbName, tableName, restoreDBName, tableName);
    }
  }
}
