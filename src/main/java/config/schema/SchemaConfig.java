package config.schema;

import lombok.Data;
import util.xml.Seed;

@Data
public class SchemaConfig {

  private Seed tableNumber;
  private Seed viewNumber;
  private TableConfig table;
  private ViewConfig view;

  public SchemaConfig() {}
}
