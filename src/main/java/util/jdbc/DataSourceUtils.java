package util.jdbc;

import adapter.Adapter;
import com.mchange.v2.c3p0.C3P0ProxyStatement;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import config.jdbc.DataSourceConfig;
import gen.data.InitialData;
import gen.data.value.AttrValue;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;

public class DataSourceUtils {

  // 是否已初始化
  private static boolean initialized;

  // 数据源
  private static ComboPooledDataSource dataSource;

  @Getter
  // 适配器
  private static Adapter adapter;

  /**
   * 获取C3P0ProxyStatement内部的SQL语句
   *
   * @param stmt stat
   * @return string of SQL
   */
  public static String getInnerStatement(PreparedStatement stmt) {

    if (stmt instanceof C3P0ProxyStatement) {
      try {
        java.lang.reflect.Method toStringMethod = java.io.PrintStream.class.getMethod("toString");
        return (String)
            ((C3P0ProxyStatement) stmt)
                .rawStatementOperation(
                    toStringMethod, C3P0ProxyStatement.RAW_STATEMENT, new Object[] {});
      } catch (NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException
          | SQLException e) {
        return stmt.toString();
      }
    }
    return adapter.getRawSQL(stmt.toString());
  }

  public static String getQuotedStatement(String rawSQL, List<AttrValue> attrValues) {
    for (AttrValue attrValue : attrValues) {
      String value = attrValue.value.toString();
      switch (attrValue.type) {
        case DECIMAL:
        case DOUBLE:
        case INTEGER:
        case BLOB:
        case BOOL:
          break;
        case VARCHAR:
        case TIMESTAMP:
        default:
          value = "\"" + value + "\"";
      }
      rawSQL = rawSQL.replaceFirst("\\?", value);
    }
    return rawSQL;
  }

  /**
   * 用原始jdbc获取 设置这个方法的目的在于：有些SQL的执行如果用c3p0创建的connection可能会存在问题，需要自己手动创建一个jdbc连接 在主线程中才需要用到这个方法
   *
   * @param isContainDB 设置jdbc中是否需要包含库的信息，有些SQL语句的执行需要包含数据库的信息，有些不需要
   * @return JDBC Connection
   * @throws SQLException 执行失败
   */
  public static Connection getJDBCConnection(boolean isContainDB) throws SQLException {

    String jdbcURL = adapter.filterJDBCUrl(dataSource.getJdbcUrl(), isContainDB);

    return DriverManager.getConnection(jdbcURL, dataSource.getUser(), dataSource.getPassword());
  }

  public static void loadCSV(InitialData initialData, String srcDir) throws SQLException {
    @Cleanup Connection conn = getJDBCConnection(true);
    for (InitialData.TableData tableData : initialData.getTableDataMap().values()) {
      String src = String.format("%s/%s.csv", srcDir, tableData.tableName);
      adapter.loadCSV(conn, tableData.tableName, tableData.columns, src);
    }
  }

  public static void loadInertList(List<String> initialData) throws SQLException {
    @Cleanup Connection conn = getJDBCConnection(true);
    ArrayList<String> list = new ArrayList<>(initialData);
    adapter.loadInsertList(conn, list);
  }

  @SneakyThrows
  public static void init(Adapter adapter, DataSourceConfig dataSourceConfig) {

    if (initialized) {
      throw new RuntimeException("DataSourceUtils不能重复初始化");
    }

    // 设置适配器
    DataSourceUtils.adapter = adapter;

    // 实例化 ComboPoolDataSource 并配置
    dataSource = new ComboPooledDataSource();
    dataSource.setDriverClass(dataSourceConfig.getDriverClassName());
    dataSource.setJdbcUrl(dataSourceConfig.getUrl());
    dataSource.setUser(dataSourceConfig.getUsername());
    dataSource.setPassword(dataSourceConfig.getPassword());

    // 加载驱动（为了适配低版本JDBC驱动）
    Class.forName(dataSource.getDriverClass());

    // 设置初始化标记
    initialized = true;
  }

  public static String getUser() {
    return dataSource.getUser();
  }

  public static String getPassword() {
    return dataSource.getPassword();
  }

  public static String getDatabase() {
    return adapter.getDatabase(dataSource.getJdbcUrl());
  }
}
