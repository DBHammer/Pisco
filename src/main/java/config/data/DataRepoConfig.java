package config.data;

import lombok.Data;
import util.xml.Seed;

@Data
public class DataRepoConfig {

  private int dataUnique;
  private Seed lenOfVarchar;
  private int numOfVarchar;
  private String charsOfVarchar;

  private int numOfInteger;
  private Seed seedOfInteger;

  private int numOfDouble;
  private Seed seedOfDouble;

  private int numOfDecimal;
  private Seed seedOfDecimal;

  private int numOfTimestamp;
  private Seed seedOfTimestamp;

  private int numOfBlob;
  private Seed lenOfBlob;

  //    public Boolean getDataUnique() {
  //        return this.dataUnique;
  //    }
}
