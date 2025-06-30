package util.rand;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class RandUtilsTest {

  @Test
  public void randSelectByProbability() {
    Map<String, Integer> probMap = new HashMap<>();
    probMap.put("One", 25);
    probMap.put("Two", 25);
    probMap.put("Three", 50);
    String randString = RandUtils.randSelectByProbability(probMap);
    System.out.println(randString);
  }
}
