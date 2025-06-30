package config.schema;

import lombok.Data;
import util.xml.Seed;

@Data
public class TableConfig {

  PrimaryKeyConfig primaryKey = null;
  UniqueKeyConfig uniqueKey = null;
  AttributeConfig attribute = null;
  ForeignKeyConfig foreignKey = null;
  IndexConfig indexConfig;
  int indexProbability;

  Seed recordNumber = null;

  boolean usePartition = false;

  boolean useRowFormat = false;

  public IndexConfig getIndexConfig() {
    if (indexConfig == null) {
      indexConfig = new IndexConfig(indexProbability);
    }
    return indexConfig;
  }

  public TableConfig() {}
}
