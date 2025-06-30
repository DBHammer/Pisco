package gen.data.param;

import config.data.DataRepoConfig;
import gen.data.type.DataType;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import lombok.Data;
import org.checkerframework.org.apache.commons.lang3.RandomStringUtils;
import util.xml.Seed;

/** ValueListRepo，要保证唯一性 */
@Data
public class DataRepo implements Serializable {
  private List<Integer> integerList;
  private List<String> varcharList;
  private List<Double> doubleList;
  private List<Double> decimalList;
  private List<byte[]> blobList;
  private List<Timestamp> timestampList;
  private List<Boolean> boolList;

  // private boolean unique;

  public DataRepo(DataRepoConfig dataRepoConfig, int unique) {
    super();

    integerList = new ArrayList<>();
    varcharList = new ArrayList<>();
    doubleList = new ArrayList<>();
    decimalList = new ArrayList<>();
    blobList = new ArrayList<>();
    timestampList = new ArrayList<>();
    boolList = new ArrayList<>();

    Random random = new Random();

    // integer
    Seed intSeed = dataRepoConfig.getSeedOfInteger();
    for (int i = 0; i < dataRepoConfig.getNumOfInteger(); i++) {
      int val = intSeed.getRandomValue();
      if (unique == 0) { // 不要求唯一性
        integerList.add(val);
      } else {
        if (!integerList.contains(val)) {
          integerList.add(val);
        }
      }
    }

    // double
    Seed doubleSeed = dataRepoConfig.getSeedOfDouble();
    for (int i = 0; i < dataRepoConfig.getNumOfDouble(); i++) {
      double val = doubleSeed.getRandomValue();
      if (unique == 0) {
        doubleList.add(val);
      } else {
        if (!doubleList.contains(val)) {
          doubleList.add(val);
        }
      }
    }

    // decimal
    Seed decimalSeed = dataRepoConfig.getSeedOfDecimal();
    for (int i = 0; i < dataRepoConfig.getNumOfDecimal(); i++) {
      double val = decimalSeed.getRandomValue();
      if (unique == 0) {
        decimalList.add(val);
      } else {
        if (!decimalList.contains(val)) {
          decimalList.add(val);
        }
      }
    }

    // varchar
    Seed lengthSeed = dataRepoConfig.getLenOfVarchar();
    Set<String> tmpChar = new HashSet<>();
    for (int i = 0; i < dataRepoConfig.getNumOfVarchar(); i++) {
      int len = lengthSeed.getRandomValue();
      String vc = RandomStringUtils.random(len, dataRepoConfig.getCharsOfVarchar());
      // 保证唯一
      tmpChar.add(vc);
    }
    varcharList.addAll(tmpChar);

    // timestamp
    Seed timestampSeed = dataRepoConfig.getSeedOfTimestamp();
    for (int i = 0; i < dataRepoConfig.getNumOfTimestamp(); i++) {
      int val = timestampSeed.getRandomValue();
      Timestamp timestamp = new Timestamp(val);
      if (!timestampList.contains(timestamp)) {
        timestampList.add(timestamp);
      }
    }

    // blob
    Seed lengthOfBlob = dataRepoConfig.getLenOfBlob();
    for (int i = 0; i < dataRepoConfig.getNumOfBlob(); i++) {
      byte[] randByte = new byte[lengthOfBlob.getRandomValue()];
      random.nextBytes(randByte);
      blobList.add(randByte);
    }

    // bool
    boolList.add(true);
    boolList.add(false);

    // 对各个ValueList排序，保证有序
    // 使得逻辑值顺序与真实值顺序一致，便于后续进行正确性验证
    Collections.sort(integerList);
    Collections.sort(varcharList);
    Collections.sort(doubleList);
    Collections.sort(decimalList);
    // Collections.sort(blobList); 不支持该类型
    Collections.sort(timestampList);
    Collections.sort(boolList);

    // 使这些List无法再被修改
    integerList = Collections.unmodifiableList(integerList);
    varcharList = Collections.unmodifiableList(varcharList);
    doubleList = Collections.unmodifiableList(doubleList);
    decimalList = Collections.unmodifiableList(decimalList);
    blobList = Collections.unmodifiableList(blobList);
    timestampList = Collections.unmodifiableList(timestampList);
    boolList = Collections.unmodifiableList(boolList);
  }

  public int getListSizeByType(DataType dataType) {
    switch (dataType) {
      case INTEGER:
        return getIntegerList().size();
      case VARCHAR:
        return getVarcharList().size();
      case DOUBLE:
        return getDoubleList().size();
      case DECIMAL:
        return getDecimalList().size();
      case BLOB:
        return getBlobList().size();
      case TIMESTAMP:
        return getTimestampList().size();
      case BOOL:
        return getBoolList().size();
      default:
        throw new RuntimeException("Unsupported data type: " + dataType.name());
    }
  }
}
