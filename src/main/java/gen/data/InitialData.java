package gen.data;

import io.IOUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import util.jdbc.DataSourceUtils;

public class InitialData {

  public static class TableData {
    public final String tableName;
    public final String columns;
    public final List<String> csvLines;

    public TableData(String tableName, String columns) {
      super();
      this.tableName = tableName;
      this.columns = columns;
      this.csvLines = new ArrayList<>();
    }
  }

  private final List<String> insertSqlList;
  // 导入数据时需要有序，所以有序存储
  @Getter private final LinkedHashMap<String, TableData> tableDataMap;

  public InitialData(List<String> insertSqlList) {
    super();
    this.insertSqlList = insertSqlList;

    // 为每一张表构造 TableData 结构
    if (insertSqlList.isEmpty()) {
      tableDataMap = null;
      return;
    }

    tableDataMap = new LinkedHashMap<>();

    // 用于从INSERT语句中匹配columns和values
    Pattern pattern = DataSourceUtils.getAdapter().getInsertPattern();

    for (String sqlLine : insertSqlList) {
      Matcher matcher = pattern.matcher(sqlLine);
      if (!matcher.find()) throw new RuntimeException("无法从插入SQL中找到初始数据");
      String tableName = matcher.group(1);
      String columns = matcher.group(2);
      String values = matcher.group(3);

      if (!tableDataMap.containsKey(tableName)) {
        tableDataMap.put(tableName, new TableData(tableName, columns));
      }

      tableDataMap.get(tableName).csvLines.add(values);
    }
  }

  public List<String> toSQLList() {
    return insertSqlList;
  }

  public void writeSQL(String dest) throws IOException {
    String sqlLines = String.join("\n", insertSqlList);
    IOUtils.writeString(sqlLines, dest, false);
  }

  public void writeCSV(String destDir) throws IOException {
    for (TableData tableData : tableDataMap.values()) {
      String dest = String.format("%s/%s.csv", destDir, tableData.tableName);
      String dataString = String.join("\n", tableData.csvLines);
      dataString = tableData.columns + "\n" + dataString;
      IOUtils.writeString(dataString, dest, false);
    }
  }
}
