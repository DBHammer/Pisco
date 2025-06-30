package util.xml;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import util.rand.RandUtils;

/**
 * 外界使用seed都通过这个类的静态方法
 *
 * @author like_
 */
public class SeedUtils {

  public static Seed initSeed(int begin, int end) {
    return new Seed(begin, end, Seed.UNIFORM_DISTRIBUTION);
  }

  public static Seed initSeed(Map<Integer, Integer> map) {
    return new Seed(map);
  }

  // check if all map keys in enum
  private static <T extends Enum<T>> boolean checkEnumValid(Set<String> keys, Class<T> enumType) {
    try {
      for (String key : keys) {
        Enum.valueOf(enumType, key);
      }
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static <T extends Enum<T>> Map<Integer, Integer> Enum2Int(
      Map<String, Object> map, Class<T> enumType) {
    HashMap<Integer, Integer> intMap = new HashMap<>();
    for (String key : map.keySet()) {
      int intKey = Enum.valueOf(enumType, key).ordinal();
      intMap.put(intKey, (int) map.get(key));
    }
    return intMap;
  }

  public static <T extends Enum<T>> Seed initSeed(Map<String, Object> map, Class<T> enumType) {
    if (map != null && enumType != null && checkEnumValid(map.keySet(), enumType)) {
      return new Seed(Enum2Int(map, enumType));
    }

    if (map == null || (!map.containsKey("seed") && !map.containsKey("type"))) {
      return initSeed(0, 1);
    }

    Integer begin, end;
    if (map.containsKey("seed")) {
      map = (Map<String, Object>) map.get("seed");
      // if null, set default param
      if (map == null) {
        return initSeed(0, 1);
      }
      // if range is null set null
      begin = (Integer) map.get("begin");
      end = (Integer) map.get("end");
      return new Seed(
          begin, end, (String) map.getOrDefault("distribution", Seed.UNIFORM_DISTRIBUTION));
    } else {
      return new Seed(
          RandUtils.getMapFromString((String) map.get("type"), (String) map.get("probability")));
    }
  }
}
