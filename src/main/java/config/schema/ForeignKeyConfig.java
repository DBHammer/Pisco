package config.schema;

import lombok.Data;
import util.xml.Seed;

@Data
public class ForeignKeyConfig {
  Seed foreignKeyNumber = null;
  Seed propPrefixFKSeed = new Seed();
  Seed pk2fkLengthSeed = new Seed();
  Seed referenceTableSeed = new Seed();

  public ForeignKeyConfig() {}
}
